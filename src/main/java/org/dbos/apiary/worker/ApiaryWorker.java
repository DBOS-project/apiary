package org.dbos.apiary.worker;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.client.InternalApiaryWorkerClient;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.postgres.PostgresRetroReplay;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import zmq.ZError;

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
        workerContext.provDBType = provenanceDatabase;
        workerContext.provAddress = provenanceAddress;
        workerContext.numWorkersThreads = numWorkerThreads;
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

    public void registerFunction(String name, String type, Callable<ApiaryFunction> function, boolean isRetro) {
        workerContext.registerFunction(name, type, function, isRetro);
    }

    public void restrictFunction(String name, Set<String> roles) {
        workerContext.restrictFunction(name, roles);
    }

    public void suspendRole(String role) {
        workerContext.suspendRole(role);
    }

    public void restoreRole(String role) {
        workerContext.restoreRole(role);
    }

    /**
     * Register a list of a functions as an execution set -- they will be executed to serve one request. Mostly used for retroactive analysis.
     * @param firstFunc     Name of the first function.
     * @param funcNames     List of functions in the set, including the first function.
     */
    public void registerFunctionSet(String firstFunc, String... funcNames) {
        workerContext.registerFunctionSet(firstFunc, funcNames);
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
                    byte[] reqBytes = InternalApiaryWorkerClient.serializeExecuteRequest(subtask.funcName, currTask.role, currTask.execId, currTask.replayMode, currCallerID, subtask.functionID, subtask.input);
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
    private void resumeExecution(long callerID, long functionID, Object output, String errorMsg) throws InterruptedException {
        ApiaryTaskStash callerTask = callerStashMap.get(callerID);
        assert (callerTask != null);
        callerTask.functionIDToValue.put(functionID, output);
        if ((errorMsg != null) && !errorMsg.isEmpty()) {
            callerTask.errorMsg += " funcId: " + functionID + ", error: " + errorMsg;
        }
        processQueuedTasks(callerTask, callerID);

        int finishedTasks = callerTask.numFinishedTasks.incrementAndGet();

        // If everything is resolved, then return the string value.
        if (finishedTasks == callerTask.totalQueuedTasks) {
            Object finalOutput = callerTask.getFinalOutput();
            assert (finalOutput != null);
            // Send back the response only once.
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerTask.callerId, callerTask.functionID,
                    callerTask.senderTimestampNano, finalOutput, callerTask.errorMsg);
            outgoingReplyMsgQueue.add(new OutgoingMsg(callerTask.replyAddr, b.build().toByteArray()));

            // Clean up the stash map.
            callerStashMap.remove(callerID);
        }
    }

    // Execute current function, push future tasks into a queue, then send back a reply if everything is finished.
    private void executeFunction(String name, String role, long execID, long callerID, long functionID, int replayMode,
                                 ZFrame replyAddr, long senderTimestampNano, Object[] arguments) throws InterruptedException {
        if (!(workerContext.getFunctionRoles(name) == null) && !workerContext.getFunctionRoles(name).contains(role)) {
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerID, functionID, senderTimestampNano, -1,
                    String.format("Current role %s has no access to function %s", role, name));
            outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));
            return;
        }
        if (workerContext.checkSuspended(role)) {
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerID, functionID, senderTimestampNano, -1,
                    String.format("Current role %s is suspended", role));
            outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));
            return;
        }
        FunctionOutput o = null;
        long tStart = System.nanoTime();
        try {
            o = callFunctionInternal(name, role, execID, functionID, replayMode, arguments);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long runtime = System.nanoTime() - tStart;
        assert (o != null);
        ApiaryTaskStash currTask = new ApiaryTaskStash(role, execID, callerID, functionID, replayMode, replyAddr, senderTimestampNano);
        currTask.output = o.output;

        // Store tasks in the list and async invoke all sub-tasks that are ready.
        // Caller ID to be passed to its subtasks;
        long currCallerID = callerIDs.incrementAndGet();
        currTask.totalQueuedTasks = o.queuedTasks.size();
        // Queue the task.
        currTask.queuedTasks.addAll(o.queuedTasks);
        currTask.errorMsg = o.errorMsg;

        processQueuedTasks(currTask, currCallerID);
        if (currTask.totalQueuedTasks != currTask.numFinishedTasks.get()) {
            // Need to store the stash map only if we have future tasks. Otherwise, we don't have to store.
            callerStashMap.put(currCallerID, currTask);
        }
        Object output = currTask.getFinalOutput();
        // If the output is not null, meaning everything is done. Directly return.
        if (output != null) {
            ExecuteFunctionReply.Builder b = Utilities.constructReply(callerID, functionID, senderTimestampNano, output, currTask.errorMsg);
            outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));
        }
    }

    private FunctionOutput callFunctionInternal(String name, String role, long execID, long functionID, int replayMode, Object[] arguments) throws Exception {
        FunctionOutput o;
        if (!workerContext.functionExists(name)) {
            logger.info("Unrecognized function: {}", name);
        }
        String type = workerContext.getFunctionType(name);
        if (type.equals(ApiaryConfig.stateless)) {
            ApiaryFunction function = workerContext.getFunction(name);
            ApiaryStatelessContext context = new ApiaryStatelessContext(workerContext, role, execID, functionID, replayMode);
            o = function.apiaryRunFunction(context, arguments);
        } else if (workerContext.getPrimaryConnectionType().equals(type)) {
            ApiaryConnection c = workerContext.getPrimaryConnection();
            o = c.callFunction(name, workerContext, role, execID, functionID, replayMode, arguments);
        } else { // Execute a read-only secondary function without primary involvement using a cached txc.
            ApiarySecondaryConnection c = workerContext.getSecondaryConnection(type);
            TransactionContext txc = workerContext.getPrimaryConnection().getLatestTransactionContext();
            // Hack, if bypass Postgres, put a committed token in the writtenKeys hashmap.
            Map<String, List<String>> writtenKeys = new HashMap<>();
            writtenKeys.putIfAbsent(MysqlContext.committedToken, new ArrayList<>());
            o = c.callFunction(name, writtenKeys, workerContext, txc, role, execID, functionID, arguments);
        }
        return o;
    }

    private void retroExecuteAll(long targetExecID, long endExecId, int replayMode, ZFrame replyAddr, long senderTimestampNano) throws Exception {
        // Turn off provenance capture for replay.
        boolean origUpdateFlag = ApiaryConfig.captureUpdates;
        boolean origReadFlag = ApiaryConfig.captureReads;
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureReads = false;

        // Currently only support Postgres retro replay.
        Object output = PostgresRetroReplay.retroExecuteAll(workerContext, targetExecID, endExecId, replayMode);

        // TODO: handle error message. Retro replay should return error message.
        ExecuteFunctionReply.Builder b = Utilities.constructReply(0l, 0l, senderTimestampNano, output, null);
        outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, b.build().toByteArray()));

        // Reset flags.
        ApiaryConfig.captureUpdates = origUpdateFlag;
        ApiaryConfig.captureReads = origReadFlag;
    }

    private class RequestRunnable implements Runnable, Comparable<RequestRunnable> {
        private final ExecuteFunctionRequest req;
        private final ZFrame address;
        public long priority;

        public RequestRunnable(ZFrame address, ExecuteFunctionRequest req) {
            this.address = address;
            this.req = req;
            try {
                this.priority = scheduler.getPriority(req.getRole(), 0);
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
                    // ExecID = 0l means the initial function, ignore.
                    workerContext.provBuff.addEntry(ApiaryConfig.tableRecordedInputs, execID, req.toByteArray());
                }
                if ((replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) || (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue())) {
                    // Must be the first function in a workflow.
                    assert (functionID == 0l);
                    long endExecId = req.getEndExecId();
                    // Retroactive replay mode goes through a separate function.
                    retroExecuteAll(execID, endExecId, replayMode, address, req.getSenderTimestampNano());
                } else {
                    executeFunction(req.getName(), req.getRole(), execID, callerID, functionID,
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
                String errorMsg = reply.getErrorMsg();
                // Resume execution.
                resumeExecution(callerID, functionID, output, errorMsg);
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
