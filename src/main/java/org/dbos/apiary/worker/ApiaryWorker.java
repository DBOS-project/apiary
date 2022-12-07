package org.dbos.apiary.worker;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.Api;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.client.InternalApiaryWorkerClient;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.postgres.PostgresUtilities;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import zmq.ZError;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ApiaryWorker {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorker.class);

    private final AtomicLong callerIDs = new AtomicLong(0);
    // Store the call stack for each caller.
    private final Map<Long, ApiaryTaskStash> callerStashMap = new ConcurrentHashMap<>();
    // Store the outgoing messages.
    private final Deque<OutgoingMsg> outgoingReqMsgQueue = new ConcurrentLinkedDeque<>();
    private final Deque<OutgoingMsg> outgoingReplyMsgQueue = new ConcurrentLinkedDeque<>();

    private final ApiaryScheduler scheduler;
    private final ZContext zContext = new ZContext(2);  // TODO: How many IO threads?
    private Thread serverThread;
    private final ExecutorService reqThreadPool;
    private final ExecutorService repThreadPool;
    private final BlockingQueue<Runnable> reqQueue = new DispatcherPriorityQueue<>();
    private final Map<String, Deque<Long>> functionRuntimesNs = new ConcurrentHashMap<>();
    private final Map<String, AtomicDouble> functionAverageRuntimesNs = new ConcurrentHashMap<>();
    private final int runningAverageLength = 100;
    private final List<Long> defaultQueue = new ArrayList<>();
    private final Long defaultTimeNs = 100000L;

    private Thread garbageCollectorThread;
    public boolean garbageCollect = true;
    public static final long gcIntervalMs = 1000;

    public final WorkerContext workerContext;

    public ApiaryWorker(ApiaryScheduler scheduler, int numWorkerThreads) {
        // By default, no provenance buffer.
        this(scheduler, numWorkerThreads, null, null);
    }

    public ApiaryWorker(ApiaryScheduler scheduler, int numWorkerThreads, String provenanceDatabase, String provenanceAddress) {
        this.scheduler = scheduler;
        reqThreadPool = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 0L, TimeUnit.MILLISECONDS, reqQueue);
        repThreadPool = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (int i = 0; i < runningAverageLength; i++) {
            defaultQueue.add(defaultTimeNs);
        }

        ProvenanceBuffer buff = null;
        try {
            buff = new ProvenanceBuffer(provenanceDatabase, provenanceAddress);
            if (!buff.hasConnection) {
                buff = null;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        workerContext = new WorkerContext(buff);
    }

    /** Public Interface **/

    public void registerConnection(String type, ApiaryConnection connection) {
        workerContext.registerConnection(type, connection);
    }

    public void registerConnection(String type, ApiarySecondaryConnection connection) {
        workerContext.registerConnection(type, connection);
    }

    public void registerFunction(String name, String type, Callable<ApiaryFunction> function) {
        workerContext.registerFunction(name, type, function);
    }

    public void startServing() {
        garbageCollectorThread = new Thread(this::garbageCollectorThread);
        garbageCollectorThread.start();
        serverThread = new Thread(this::serverThread);
        serverThread.start();
    }

    public void shutdown() {
        try {
            garbageCollect = false;
            Thread.sleep(100);
            if (garbageCollectorThread != null) {
                garbageCollectorThread.interrupt();
                garbageCollectorThread.join();
            }
            reqThreadPool.shutdown();
            reqThreadPool.awaitTermination(10, TimeUnit.SECONDS);
            repThreadPool.shutdown();
            repThreadPool.awaitTermination(10, TimeUnit.SECONDS);
            if (serverThread != null) {
                serverThread.interrupt();
            }
            zContext.close();
            if (serverThread != null) {
                serverThread.join();
            }
            if (workerContext.provBuff != null) {
                workerContext.provBuff.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** Private Methods **/

    private void garbageCollectorThread() {
        while (garbageCollect && ApiaryConfig.XDBTransactions) {
            Set<TransactionContext> activeTransactions = workerContext.getPrimaryConnection().getActiveTransactions();
            for (String secondary : workerContext.secondaryConnections.keySet()) {
                ApiarySecondaryConnection c = workerContext.getSecondaryConnection(secondary);
                c.garbageCollect(activeTransactions);
            }
            try {
                Thread.sleep(gcIntervalMs);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void processQueuedTasks(ApiaryTaskStash currTask, long currCallerID) {
        int numTraversed = 0;
        int totalTasks = currTask.queuedTasks.size();
        while (!currTask.queuedTasks.isEmpty()) {
            try {
                Task subtask = currTask.queuedTasks.peek();
                if (subtask == null) {
                    break;
                }
                // Run all tasks that have no dependencies.
                if (subtask.dereferenceFutures(currTask.functionIDToValue)) {
                    boolean removed = currTask.queuedTasks.remove(subtask);
                    if (!removed) {
                        continue;
                    }
                    String address = workerContext.getFunctionType(subtask.funcName).equals(ApiaryConfig.stateless) ?
                            workerContext.getPrimaryConnection().getPartitionHostMap().get(0)
                            : workerContext.getPrimaryConnection().getHostname(subtask.input);
                    // Push to the outgoing queue.
                    if (ApiaryConfig.workerAsyncDelay) {
                        // Add some delay if we are trying to do fault injection.
                        Thread.sleep(ThreadLocalRandom.current().nextInt(10));
                    }
                    byte[] reqBytes = InternalApiaryWorkerClient.serializeExecuteRequest(subtask.funcName, currTask.service, currTask.execId, currTask.replayMode, currCallerID, subtask.functionID, subtask.input);
                    outgoingReqMsgQueue.add(new OutgoingMsg(address, reqBytes));
                }
                numTraversed++;
                if (numTraversed >= totalTasks) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    // Resume the execution of the caller function, then send back a reply if everything is finished.
    private void resumeExecution(long callerID, long functionID, Object output) throws InterruptedException {
        ApiaryTaskStash callerTask = callerStashMap.get(callerID);
        assert (callerTask != null);
        callerTask.functionIDToValue.put(functionID, output);
        processQueuedTasks(callerTask, callerID);

        int finishedTasks = callerTask.numFinishedTasks.incrementAndGet();

        // If everything is resolved, then return the string value.
        if (finishedTasks == callerTask.totalQueuedTasks) {
            Object finalOutput = callerTask.getFinalOutput();
            assert (finalOutput != null);
            // Send back the response only once.
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerTask.callerId, callerTask.functionID,
                    callerTask.senderTimestampNano, finalOutput);
            outgoingReplyMsgQueue.add(new OutgoingMsg(callerTask.replyAddr, b.build().toByteArray()));

            // Clean up the stash map.
            callerStashMap.remove(callerID);
        }
    }

    // Execute current function, push future tasks into a queue, then send back a reply if everything is finished.
    private void executeFunction(String name, String service, long execID, long callerID, long functionID, int replayMode,
                                 ZFrame replyAddr, long senderTimestampNano, Object[] arguments) throws InterruptedException {
        FunctionOutput o = null;
        long tStart = System.nanoTime();
        try {
            o = callFunctionInternal(name, service, execID, functionID, replayMode, arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long runtime = System.nanoTime() - tStart;
        assert (o != null);
        ApiaryTaskStash currTask = new ApiaryTaskStash(service, execID, callerID, functionID, replayMode, replyAddr, senderTimestampNano);
        currTask.output = o.output;

        // Store tasks in the list and async invoke all sub-tasks that are ready.
        // Caller ID to be passed to its subtasks;
        long currCallerID = callerIDs.incrementAndGet();
        currTask.totalQueuedTasks = o.queuedTasks.size();
        // Queue the task.
        currTask.queuedTasks.addAll(o.queuedTasks);

        processQueuedTasks(currTask, currCallerID);
        if (currTask.totalQueuedTasks != currTask.numFinishedTasks.get()) {
            // Need to store the stash map only if we have future tasks. Otherwise, we don't have to store.
            callerStashMap.put(currCallerID, currTask);
        }
        Object output = currTask.getFinalOutput();
        // If the output is not null, meaning everything is done. Directly return.
        if (output != null) {
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerID, functionID, senderTimestampNano, output);
            outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));
        }
        // Record runtime.
        functionRuntimesNs.putIfAbsent(name, new ConcurrentLinkedDeque<>(defaultQueue));
        functionAverageRuntimesNs.putIfAbsent(name, new AtomicDouble((double) defaultTimeNs));
        Deque<Long> times = functionRuntimesNs.get(name);
        times.offerFirst(runtime);
        long old = times.pollLast();
        functionAverageRuntimesNs.get(name).getAndAdd(((double) (runtime - old)) / runningAverageLength);
    }

    private FunctionOutput callFunctionInternal(String name, String service, long execID, long functionID, int replayMode, Object[] arguments) throws Exception {
        FunctionOutput o;
        if (!workerContext.functionExists(name)) {
            logger.info("Unrecognized function: {}", name);
        }
        String type = workerContext.getFunctionType(name);
        if (type.equals(ApiaryConfig.stateless)) {
            ApiaryFunction function = workerContext.getFunction(name);
            ApiaryStatelessContext context = new ApiaryStatelessContext(workerContext, service, execID, functionID, replayMode);
            o = function.apiaryRunFunction(context, arguments);
        } else if (workerContext.getPrimaryConnectionType().equals(type)) {
            ApiaryConnection c = workerContext.getPrimaryConnection();
            o = c.callFunction(name, workerContext, service, execID, functionID, replayMode, arguments);
        } else { // Execute a read-only secondary function without primary involvement using a cached txc.
            ApiarySecondaryConnection c = workerContext.getSecondaryConnection(type);
            TransactionContext txc = workerContext.getPrimaryConnection().getLatestTransactionContext();
            // Hack, if bypass Postgres, put a committed token in the writtenKeys hashmap.
            Map<String, List<String>> writtenKeys = new HashMap<>();
            writtenKeys.putIfAbsent(MysqlContext.committedToken, new ArrayList<>());
            o = c.callFunction(name, writtenKeys, workerContext, txc, service, execID, functionID, arguments);
        }
        return o;
    }

    private void retroExecuteAll(long targetExecID, int replayMode, ZFrame replyAddr, long senderTimestampNano) throws Exception {
        logger.info("retro execute all!");
        assert(workerContext.provBuff != null);
        Connection conn = workerContext.provBuff.conn.get();

        // Turn off provenance capture for replay.
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureReads = false;

        // Find previous execution history, only execute later committed transactions.
        // TODO: maybe re-execute aborted transaction, especially for bug reproduction?
        String provQuery = String.format("SELECT %s, %s FROM %s WHERE %s = %d AND %s=0 AND %s=0 AND %s=\'%s\';",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID, ApiaryConfig.tableFuncInvocations,
                ProvenanceBuffer.PROV_EXECUTIONID, targetExecID, ProvenanceBuffer.PROV_FUNCID,
                ProvenanceBuffer.PROV_ISREPLAY, ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT);
        Statement stmt = conn.createStatement();
        ResultSet historyRs = stmt.executeQuery(provQuery);
        long origTxid = -1;
        long origExecId = -1;
        if (historyRs.next()) {
            origTxid = historyRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            origExecId = historyRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        } else {
            logger.error("No corresponding original transaction for execution {}", targetExecID);
            throw new RuntimeException("Cannot find original transaction!");
        }
        historyRs.close();

        // Replay based on the snapshot info, because transaction/commit order != actual serial order.
        // Start transactions based on their original txid order, but commit based on commit order. And maintain a pool of connections to the backend database to concurrently execute transactions.

        // This query arranges the starting order of transactions.
        String startOrderQuery = String.format("SELECT * FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;", ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        ResultSet startOrderRs = stmt.executeQuery(startOrderQuery);
        assert (startOrderRs.next());  // Should have at least one execution.

        // This query finds the commit order of transactions.
        String commitOrderQuery = String.format("SELECT %s, %s FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;", ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT, ProvenanceBuffer.PROV_END_TIMESTAMP);
        Statement commitOrderStmt = conn.createStatement();
        ResultSet commitOrderRs = commitOrderStmt.executeQuery(commitOrderQuery);

        // This query finds the original input.
        String inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                        "WHERE %s >= %d AND %s = 0 AND %s = 0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                origTxid, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
        );
        Statement inputStmt = conn.createStatement();
        ResultSet inputRs = inputStmt.executeQuery(inputQuery);

        // Cache inputs of the original execution. <execId, input>
        long currInputExecId = -1;
        Object[] currInputs = null;

        // Store currently unresolved tasks. <execId, funcId, task>
        Map<Long, Map<Long, Task>> pendingTasks = new HashMap<>();

        // Store funcID to value mapping of each execution.
        Map<Long, Map<Long, Object>> execFuncIdToValue = new HashMap<>();

        // Store execID to final output map. Because the output could be a future.
        Map<Long, Object> execIdToFinalOutput = new HashMap<>();

        // A connection pool to the backend database. For concurrent executions.
        int connPoolSize = 10;  // Connection pool size. TODO: tune this.
        Queue<Connection> connPool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < connPoolSize; i++) {
            connPool.add(workerContext.getPrimaryConnection().getRawConnection());
        }

        // A pending commit map from transaction ID to connection. <txid, connection>
        Map<Long, Connection> pendingCommits = new HashMap<>();

        // Next commit transaction ID, the next to be committed.
        assert (commitOrderRs.next());
        long nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);

        while (nextCommitTxid > 0) {
            // Execute until nextCommitTxid is in the snapshot of that original transaction.
            while (true) {
                long resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
                long resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
                long resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
                String[] resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
                String resName = resNames[resNames.length - 1]; // Extract the actual function name.
                logger.info("CHECKING txid {}, execid {}, funcid {}, name {}", resTxId, resExecId, resFuncId, resName);
                String resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
                long xmax = PostgresUtilities.parseXmax(resSnapshotStr);
                List<Long> activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);
                if ((resTxId == nextCommitTxid) || (nextCommitTxid >= xmax) || (activeTxns.contains(nextCommitTxid))) {
                    // Not in its snapshot. Start a new transaction.
                    Connection currConn = connPool.poll();
                    if (currConn == null) {
                        throw new RuntimeException("Not enough connections to replay!");
                    }

                    // Execute the function.
                    // Get inputs.
                    if ((resExecId != currInputExecId) && (resFuncId == 0l)) {
                        // Read the input for this execution ID.
                        if (inputRs.next()) {
                            currInputExecId = inputRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
                            byte[] recordInput = inputRs.getBytes(ProvenanceBuffer.PROV_REQ_BYTES);
                            ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(recordInput);
                            currInputs = Utilities.getArgumentsFromRequest(req);
                            if (currInputExecId != resExecId) {
                                logger.error("Input execID {} does not match the expected ID {}!", currInputExecId, resExecId);
                                throw new RuntimeException("Retro replay failed due to mismatched IDs.");
                            }
                            logger.info("Original arguments execid {}, inputs {}", currInputExecId, currInputs);
                        } else {
                            logger.error("Could not find the input for this execution ID {} ", resExecId);
                            throw new RuntimeException("Retro replay failed due to missing input.");
                        }
                    }

                    // TODO: optimize for empty transactions?
                    processReplayFunction(currConn, resExecId, resFuncId, resName, replayMode, currInputs, pendingTasks, execFuncIdToValue, execIdToFinalOutput);
                    pendingCommits.put(resTxId, currConn);
                    if (!startOrderRs.next()) {
                        // No more to process.
                        break;
                    }
                } else {
                    break;  // Need to wait until nextCommitTxid to commit.
                }
            }

            // Commit the nextCommitTxid and update the variables.
            Connection commitConn = pendingCommits.get(nextCommitTxid);
            assert (commitConn != null);
            commitConn.commit();
            connPool.add(commitConn);
            pendingCommits.remove(nextCommitTxid);
            if (commitOrderRs.next()) {
                nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            } else {
                nextCommitTxid = 0;
            }
        }

        if (!pendingTasks.isEmpty()) {
            throw new RuntimeException("Still more pending tasks to be solved! Currently do not support adding transactions.");
        }

        Object output = execIdToFinalOutput.get(currInputExecId);  // The last execution ID.
        ExecuteFunctionReply.Builder b = Utilities.constructReply(0l, 0l, senderTimestampNano, output);
        outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));

        // Clean up connection pool.
        while (!connPool.isEmpty()) {
            Connection currConn = connPool.poll();
            if (currConn != null) {
                currConn.close();
            }
        }
    }

    // Return true if executed the function, false if nothing executed.
    private boolean processReplayFunction(Connection conn, long execId, long funcId, String funcName, int replayMode, Object[] inputs, Map<Long, Map<Long, Task>> pendingTasks,
                                          Map<Long, Map<Long, Object>> execFuncIdToValue,
                                          Map<Long, Object> execIdToFinalOutput) throws Exception {
        FunctionOutput fo;
        // Only support primary functions.
        if (!workerContext.functionExists(funcName)) {
            logger.info("Unrecognized function: {}, cannot replay, skipped.", funcName);
            return false;
        }
        String type = workerContext.getFunctionType(funcName);
        if (!workerContext.getPrimaryConnectionType().equals(type)) {
            logger.error("Replay only support primary functions!");
            throw new RuntimeException("Replay only support primary functions!");
        }

        ApiaryConnection c = workerContext.getPrimaryConnection();

        if (funcId == 0l) {
            // This is the first function of a request.
            fo = c.replayCallFunction(conn, funcName, workerContext, "retroReplay", execId, funcId, replayMode, inputs);
            assert (fo != null);
            execFuncIdToValue.putIfAbsent(execId, new HashMap<>());
            execIdToFinalOutput.putIfAbsent(execId, fo.output);
            pendingTasks.putIfAbsent(execId, new HashMap<>());
        } else {
            // Skip the task if it is absent. Because we allow reducing the number of called functions (currently does not support adding more).
            if (!pendingTasks.containsKey(execId) || !pendingTasks.get(execId).containsKey(funcId)) {
                logger.info("Skip function ID {}, not found in pending tasks.", funcId);
                return false;
            }
            // Find the task in the stash. Make sure that all futures have been resolved.
            Task currTask = pendingTasks.get(execId).get(funcId);

            // Resolve input for this task. Must success.
            Map<Long, Object> currFuncIdToValue = execFuncIdToValue.get(execId);

            if (!currTask.dereferenceFutures(currFuncIdToValue)) {
                logger.error("Failed to dereference input for execId {}, funcId {}. Aborted", execId, funcId);
                throw new RuntimeException("Retro replay failed to dereference input.");
            }

            fo = c.replayCallFunction(conn, currTask.funcName, workerContext,  "retroReplay", execId, funcId, replayMode, currTask.input);
            assert (fo != null);
            // Remove this task from the map.
            pendingTasks.get(execId).remove(funcId);
        }
        // Store output value.
        execFuncIdToValue.get(execId).putIfAbsent(funcId, fo.output);
        // Queue all of its async tasks to the pending map.
        for (Task t : fo.queuedTasks) {
            if (pendingTasks.get(execId).containsKey(t.functionID)) {
                logger.error("ExecID {} funcID {} has duplicated outputs!", execId, t.functionID);
            }
            pendingTasks.get(execId).putIfAbsent(t.functionID, t);
        }

        if (pendingTasks.get(execId).isEmpty()) {
            // Check if we need to update the final output map.
            Object o = execIdToFinalOutput.get(execId);
            if (o instanceof ApiaryFuture) {
                ApiaryFuture futureOutput = (ApiaryFuture) o;
                assert (execFuncIdToValue.get(execId).containsKey(futureOutput.futureID));
                Object resFo = execFuncIdToValue.get(execId).get(futureOutput.futureID);
                execIdToFinalOutput.put(execId, resFo);
            }
            // Clean up.
            execFuncIdToValue.remove(execId);
            pendingTasks.remove(execId);
        }
        return true;
    }

    private class RequestRunnable implements Runnable, Comparable<RequestRunnable> {
        private final ExecuteFunctionRequest req;
        private final ZFrame address;
        public long priority;

        public RequestRunnable(ZFrame address, ExecuteFunctionRequest req) {
            this.address = address;
            this.req = req;
            try {
                long runtime = functionAverageRuntimesNs.getOrDefault(req.getName(), new AtomicDouble(defaultTimeNs)).longValue();
                this.priority = scheduler.getPriority(req.getService(), runtime);
            } catch (AssertionError | Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            // Handle the request.
            try {
                scheduler.onDequeue(req);
                assert (req != null);

                long callerID = req.getCallerId();
                long functionID = req.getFunctionId();
                long execID = req.getExecutionId();
                int replayMode = req.getReplayMode();
                Object[] arguments = Utilities.getArgumentsFromRequest(req);

                if (ApiaryConfig.recordInput &&
                        (replayMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) &&
                        (callerID == 0L) && (execID != 0L) &&
                        (workerContext.provBuff != null)) {
                    // Log function input if recordInput is set to true, during initial execution, and if this is the first function of the entire workflow.
                    // ExecID = 0l means the initial service function, ignore.
                    workerContext.provBuff.addEntry(ApiaryConfig.tableRecordedInputs, execID, req.toByteArray());
                }
                if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
                    // Must be the first function in a workflow.
                    assert (functionID == 0l);
                    // Retroactive replay mode goes through a separate function.
                    retroExecuteAll(execID, replayMode, address, req.getSenderTimestampNano());
                } else {
                    executeFunction(req.getName(), req.getService(), execID, callerID, functionID,
                            replayMode, address, req.getSenderTimestampNano(), arguments);
                }
            } catch (AssertionError | Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public int compareTo(RequestRunnable requestRunnable) {
            return Long.compare(priority, requestRunnable.priority);
        }
    }

    private class ReplyRunnable implements Runnable {
        private final byte[] replyBytes;

        public ReplyRunnable(byte[] reply) {
            this.replyBytes = reply;
        }

        @Override
        public void run() {
            // Handle the reply.
            try {
                ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                Object output = Utilities.getOutputFromReply(reply);
                long callerID = reply.getCallerId();
                long functionID = reply.getFunctionId();
                // Resume execution.
                resumeExecution(callerID, functionID, output);
            } catch (InvalidProtocolBufferException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
        // Set high water mark to unbounded, so we can have unlimited outstanding messages.
        // TODO: it may be better to add a bound.
        frontend.setHWM(0);
        frontend.setRouterMandatory(true);
        frontend.bind("tcp://*:" + ApiaryConfig.workerPort);

        // This main server thread is used as I/O thread.
        InternalApiaryWorkerClient client = new InternalApiaryWorkerClient(shadowContext);
        List<String> distinctHosts = workerContext.getPrimaryConnection().getPartitionHostMap()
                .values().stream().distinct().collect(Collectors.toList());
        ZMQ.Poller poller = zContext.createPoller(distinctHosts.size() + 1);
        // The backend worker is always the first poller socket.
        poller.register(frontend, ZMQ.Poller.POLLIN);
        // Populate sockets for all remote workers in the cluster.

        for (String hostname : distinctHosts) {
            ZMQ.Socket socket = client.getSocket(hostname);
            poller.register(socket, ZMQ.Poller.POLLIN);
        }

        while (!Thread.currentThread().isInterrupted()) {
            // Poll sockets with timeout 1ms.
            // TODO: why poll() is expensive? poll(0) had lower performance.
            int prs = poller.poll(1);
            if (prs == -1) {
                break;
            }

            // Handle request from clients or other workers.
            if (poller.pollin(0)) {
                try {
                    ZMsg msg = ZMsg.recvMsg(frontend);
                    ZFrame address = msg.pop();
                    ZFrame content = msg.poll();
                    assert (content != null);
                    msg.destroy();
                    byte[] reqBytes = content.getData();
                    ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(reqBytes);
                    reqThreadPool.execute(new RequestRunnable(address, req));
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                        break;
                    } else {
                        e.printStackTrace();
                    }
                } catch (Exception | AssertionError e) {
                    e.printStackTrace();
                }
            }

            // Handle reply from requests.
            for (int i = 1; i < poller.getSize(); i++) {
                if (poller.pollin(i)) {
                    try {
                        String hostname = distinctHosts.get(i-1);
                        ZMQ.Socket socket = client.getSocket(hostname);
                        ZMsg msg = ZMsg.recvMsg(socket);
                        ZFrame content = msg.getLast();
                        assert (content != null);
                        byte[] replyBytes = content.getData();
                        msg.destroy();

                        repThreadPool.execute(new ReplyRunnable(replyBytes));
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                            break;
                        } else {
                            e.printStackTrace();
                        }
                    } catch (Exception | AssertionError e) {
                        e.printStackTrace();
                    }
                }
            }

            // Handle reply to send back.
            // TODO: do we send back all of those, or just send back a few?
            while (!outgoingReplyMsgQueue.isEmpty()) {
                OutgoingMsg msg = outgoingReplyMsgQueue.poll();
                boolean sent;
                try {
                    assert (msg.hostname == null);
                    assert (msg.address != null);
                    sent = msg.address.send(frontend, ZFrame.REUSE | ZFrame.MORE | ZMQ.DONTWAIT);
                    if (!sent) {
                        int errno = frontend.errno();
                        logger.info("Frontend replyAddress failed to send, errno == {}", errno);
                        if (errno != ZError.EAGAIN) {
                            // Ignore the error.
                            continue;
                        } else {
                            outgoingReplyMsgQueue.addFirst(msg);
                            break;
                        }
                    }
                    ZFrame replyContent = new ZFrame(msg.output);
                    sent = replyContent.send(frontend, ZMQ.DONTWAIT);
                    assert (sent);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Continue processing.");
                }
            }

            while (!outgoingReqMsgQueue.isEmpty()) {
                OutgoingMsg msg = outgoingReqMsgQueue.poll();
                boolean sent;
                try {
                    assert  (msg.hostname != null);
                    ZMQ.Socket socket = client.getSocket(msg.hostname);
                    sent = socket.send(msg.output, ZMQ.DONTWAIT);
                    if (!sent) {
                        // Something went wrong.
                        int errno = socket.errno();
                        logger.info("Socket Failed to send, errno == {}", errno);
                        if (errno != ZError.EAGAIN) {
                            // Ignore the error.
                            continue;
                        } else {
                            outgoingReqMsgQueue.addFirst(msg);
                            break;
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Continue processing.");
                }
            }
        }

        poller.close();
        shadowContext.close();
    }
}
