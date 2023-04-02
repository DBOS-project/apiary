package org.dbos.apiary.postgres;

import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PostgresRetroReplay {
    private static final Logger logger = LoggerFactory.getLogger(PostgresRetroReplay.class);

    // Record a list of modified tables. Track dependencies for replaying requests.
    public static final Set<String> replayWrittenTables = ConcurrentHashMap.newKeySet();

    // Cache accessed tables of a function set. <firstFuncName, String[]>
    private static final Map<String, String[]> funcSetAccessTables = new ConcurrentHashMap<>();

    // Store currently unresolved tasks. <execId, funcId, task>
    public static final Map<Long, Map<Long, Task>> pendingTasks = new ConcurrentHashMap<>();

    // Store funcID to value mapping of each execution.
    public static final Map<Long, Map<Long, Object>> execFuncIdToValue = new ConcurrentHashMap<>();

    // Store execID to final output map. Because the output could be a future.
    // TODO: garbage collect this map.
    public static final Map<Long, Object> execIdToFinalOutput = new ConcurrentHashMap<>();

    // A pending commit map from original transaction ID to Postgres replay task.
    private static final Map<Long, PostgresReplayTask> pendingCommitTasks = new ConcurrentHashMap<>();

    // Record the last executed transaction for this request. Make sure we commit it before launching the next one.
    private static final Map<Long, Long> execIdLastExecTxn = new ConcurrentHashMap<>();

    private static final Collection<Long> commitTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> prepareTxnTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> submitTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> totalTimes = new ConcurrentLinkedQueue<>();

    private static void resetReplayState() {
        replayWrittenTables.clear();
        funcSetAccessTables.clear();
        pendingTasks.clear();
        execFuncIdToValue.clear();
        execIdToFinalOutput.clear();
        pendingCommitTasks.clear();
        execIdLastExecTxn.clear();
        commitTimes.clear();
        prepareTxnTimes.clear();
        submitTimes.clear();
        totalTimes.clear();
    }

    public static Object retroExecuteAll(WorkerContext workerContext, long targetExecID, long endExecId, int replayMode) throws Exception {
        // Clean up.
        resetReplayState();
        assert(workerContext.provBuff != null);
        Connection provConn = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);

        long startTime = System.currentTimeMillis();
        Set<String> nonSkipFuncs = workerContext.listAllFunctions();
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            logger.debug("Replay the entire trace!");
        } else if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
            logger.debug("Selective replay!");
            // Get a list of functions that must be executed.
            nonSkipFuncs = getExecFuncSets(workerContext);
        } else {
            logger.error("Do not support replay mode: {}", replayMode);
            return null;
        }

        // Find previous execution history, only execute later committed transactions or the ones failed due to unrecoverable issues (like constraint violations).
        String provQuery = String.format("SELECT %s, %s FROM %s WHERE %s = ? AND %s=0 AND %s=0 AND (%s=\'%s\' OR %s=\'%s\');",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID,
                ProvenanceBuffer.PROV_ISREPLAY, ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
        PreparedStatement stmt = provConn.prepareStatement(provQuery);
        stmt.setLong(1, targetExecID);
        ResultSet historyRs = stmt.executeQuery();
        long startTxId = -1;
        if (historyRs.next()) {
            startTxId = historyRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay start transaction ID: {}", startTxId);
        } else {
            logger.error("No corresponding original transaction for start execution {}", targetExecID);
            throw new RuntimeException("Cannot find original transaction!");
        }
        historyRs.close();

        // Find the transaction ID of the last execution. Only need to find the transaction ID of the first function.
        // Execute everything between [startTxId, endTxId)
        stmt.setLong(1, endExecId);
        ResultSet endRs = stmt.executeQuery();
        long endTxId = Long.MAX_VALUE;
        if (endRs.next()) {
            endTxId = endRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay end transaction ID (excluded): {}", endTxId);
        } else {
            logger.debug("No corresponding original transaction for end execution {}. Execute the entire trace!", endExecId);
        }

        // This query finds the starting order of transactions.
        // Replay mode only consider committed transactions.
        // APIARY_TRANSACTION_ID, APIARY_EXECUTIONID, APIARY_FUNCID, APIARY_PROCEDURENAME, APIARY_TXN_SNAPSHOT
        String startOrderQuery = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s >= ? AND %s < ? AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_PROCEDURENAME, ProvenanceBuffer.PROV_TXN_SNAPSHOT,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        if (workerContext.hasRetroFunctions()) {
            // Include aborted transactions for retroaction.
            startOrderQuery = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s >= ? AND %s < ? AND %s=0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_PROCEDURENAME, ProvenanceBuffer.PROV_TXN_SNAPSHOT,
                    ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        }
        PreparedStatement startOrderStmt = provConn.prepareStatement(startOrderQuery);
        startOrderStmt.setLong(1, startTxId);
        startOrderStmt.setLong(2, endTxId);
        ResultSet startOrderRs = startOrderStmt.executeQuery();

        // This query finds the original input.
        String inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                        "WHERE %s >= ? AND %s < ? AND %s = 0 AND %s = 0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
        );
        if (workerContext.hasRetroFunctions()) {
            inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                            "WHERE %s >= ? AND %s < ? AND %s = 0 AND %s = 0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                    ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                    ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                    ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
            );
        }
        Connection provInputConn = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);
        PreparedStatement inputStmt = provInputConn.prepareStatement(inputQuery);
        inputStmt.setLong(1, startTxId);
        inputStmt.setLong(2, endTxId);
        ResultSet inputRs = inputStmt.executeQuery();

        // Cache inputs of the original execution. <execId, input>
        long currInputExecId = -1;
        Object[] currInputs = null;

        long lastNonSkippedExecId = -1;  // The last not-skipped execution ID. Useful to decide the final output.

        // A connection pool to the backend database. For concurrent executions.
        Queue<Connection> connPool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < workerContext.numWorkersThreads; i++) {
            connPool.add(workerContext.getPrimaryConnection().createNewConnection());
        }

        // A thread pool for concurrent function executions.
        ExecutorService threadPool = Executors.newFixedThreadPool(workerContext.numWorkersThreads);
        ExecutorService commitThreadPool = Executors.newCachedThreadPool();

        long prepTime = System.currentTimeMillis();
        logger.info("Prepare time: {} ms", prepTime - startTime);

        // Main loop: start based on the transaction ID order and the snapshot info, Postgres commit timestamp may not be reliable.
        // Start transactions based on their original txid order, but commit it before executing the first transaction that has it in the snapshot.
        // Maintain a pool of connections to the backend database to concurrently execute transactions.
        int totalStartOrderTxns = 0;
        int totalExecTxns = 0;
        List<Long> checkVisibleTxns = new ArrayList<>(); // Committed but not guaranteed to be visible yet.
        while (startOrderRs.next()) {
            long t0 = System.nanoTime();
            totalStartOrderTxns++;
            long resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            long resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
            long resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
            String[] resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
            String resName = resNames[resNames.length - 1]; // Extract the actual function name.
            String resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
            long xmax = PostgresUtilities.parseXmax(resSnapshotStr);
            List<Long> activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);
            logger.debug("Processing txnID {}, execId {}, funcId {}, funcName {}", resTxId, resExecId, resFuncId, resName);

            // Get the input for this transaction.
            if ((resExecId != currInputExecId) && (resFuncId == 0L)) {
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
                    logger.debug("Original arguments execid {}, inputs {}", currInputExecId, currInputs);
                } else {
                    logger.error("Could not find the input for this execution ID {} ", resExecId);
                    throw new RuntimeException("Retro replay failed due to missing input.");
                }
            }

            // Check if we can skip this function execution. If so, add to the skip list. Otherwise, execute the replay.
            if (!nonSkipFuncs.contains(resName) && (lastNonSkippedExecId != -1)) {
                // Do not skip the first execution.
                logger.debug("Skipping function {}: transaction {}, execution ID {}", resName, resTxId, resExecId);
                continue;
            }

            // Check if we need to commit anything.
            Map<Long, Future<Long>> cleanUpTxns = new HashMap<>();

            // If it is not the first function, make sure we wait its previous tasks to finish.
            long lastExecTxnId = -1L;
            if (resFuncId > 0) {
                lastExecTxnId = execIdLastExecTxn.get(resExecId); // Must be not null.
            }
            for (long cmtTxn : pendingCommitTasks.keySet()) {
                PostgresReplayTask commitPgRpTask = pendingCommitTasks.get(cmtTxn);
                if (commitPgRpTask == null) {
                    logger.error("No task found for pending commit txn {}.", cmtTxn);
                    throw new RuntimeException("Failed to find commit transaction " + cmtTxn);
                }
                boolean processed = false;
                // If this transaction is in resTxId's snapshot, then wait and commit it.
                if ((cmtTxn < xmax) && !activeTxns.contains(cmtTxn)) {
                    logger.debug("Committing txnID {} because in the snapshot of txn {}", cmtTxn, resTxId);
                    // If it's a non-dependent read-only transaction, do not submit a commit task.
                    if (workerContext.getFunctionReadOnly(commitPgRpTask.task.funcName) && (cmtTxn != lastExecTxnId)) {
                        // Wait if it's the dependent task.
                        logger.debug("Don't wait for non-dependent read-only transaction {}.", cmtTxn);
                    } else {
                        Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(commitPgRpTask, workerContext, cmtTxn, replayMode, threadPool));
                        cleanUpTxns.put(cmtTxn, cmtFut);
                        // Use the new transaction ID! Not their original ones.
                        // Only wait for write transactions.
                        checkVisibleTxns.add(commitPgRpTask.replayTxnID);
                        processed = true; // Mark as processed, do not repeat a read-only commit.
                    }
                }
                if (!processed && workerContext.getFunctionReadOnly(commitPgRpTask.task.funcName) && commitPgRpTask.resFut.isDone()) {
                    // If it's a read-only transaction and has finished, but not in its snapshot, still release the resources immediately.
                    if (commitPgRpTask.resFut.isDone()) {
                        logger.debug("Clean up read-only txnID {} for current txnID {}", cmtTxn, resTxId);
                        cleanUpTxns.put(cmtTxn, CompletableFuture.completedFuture(cmtTxn));
                    }
                }
            }
            // Clean up pending commit map.
            for (long cmtTxn : cleanUpTxns.keySet()) {
                try {
                    long retVal = cleanUpTxns.get(cmtTxn).get(5, TimeUnit.SECONDS);
                    if (retVal != cmtTxn) {
                        logger.error("Commit failed. Commit txn: {}, retVal: {}", cmtTxn, retVal);
                    }
                } catch (TimeoutException e) {
                    logger.error("Commit timed out for transaction {}", cmtTxn);
                    // TODO: better error handling? Abort connection?
                }
                connPool.add(pendingCommitTasks.get(cmtTxn).conn);
                pendingCommitTasks.remove(cmtTxn);
            }
            long t1 = System.nanoTime();
            if (!cleanUpTxns.isEmpty()) {
                commitTimes.add(t1 - t0);
            }

            // Execute this transaction.
            Task rpTask = new Task(resExecId, resFuncId, resName, currInputs);
            execIdLastExecTxn.put(resExecId, resTxId);

            // Skip the task if it is absent. Because we allow reducing the number of called function. Should never have race condition because later tasks must have previous tasks in their snapshot.
            if ((rpTask.functionID > 0L) && (!pendingTasks.containsKey(rpTask.execId) || !pendingTasks.get(rpTask.execId).containsKey(rpTask.functionID))) {
                if (workerContext.hasRetroFunctions()) {
                    logger.debug("Skip execution ID {}, function ID {}, not found in pending tasks.", rpTask.execId, rpTask.functionID);
                } else {
                    logger.error("Not found execution ID {}, function ID {} in pending tasks. Should not happen in replay!", rpTask.execId, rpTask.functionID);
                }
                continue;
            }

            totalExecTxns++;
            lastNonSkippedExecId = resExecId;
            Connection currConn = connPool.poll();
            if (currConn == null) {
                // Allocate more connections.
                currConn = workerContext.getPrimaryConnection().createNewConnection();
            }
            PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);

            // Because Postgres may commit a transaction but take a while for it to show up in the snapshot for the following transactions, wait until we get everything from checkVisibleTxns in the snapshot.
            // We can wait by committing the empty transaction and create a new pgCtxt.
            PostgresContext pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, ApiaryConfig.systemRole, pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
            boolean allVisible = false;
            while (!allVisible) {
                logger.debug("Checking visible transactions: {}. Current transaction id {}, xmin {}, xmax {}, active transactions {}", checkVisibleTxns.toString(), pgCtxt.txc.txID, pgCtxt.txc.xmin, pgCtxt.txc.xmax, pgCtxt.txc.activeTransactions.toString());
                allVisible = true;
                List<Long> visibleTxns = new ArrayList<>();
                for (long replayCmtTxn : checkVisibleTxns) {
                    // Check if the committed transaction does not show up in the snapshot.
                    if ((replayCmtTxn >= pgCtxt.txc.xmax) || pgCtxt.txc.activeTransactions.contains(replayCmtTxn)) {
                        logger.debug("Transaction {} still not visible. xmax {}, activetransactions: {}", replayCmtTxn, pgCtxt.txc.xmax, pgCtxt.txc.activeTransactions.toString());
                        allVisible = false;
                    } else {
                        visibleTxns.add(replayCmtTxn);  // Record visible.
                    }
                }
                checkVisibleTxns.removeAll(visibleTxns);
                if (!allVisible) {
                    try {
                        pgCtxt.conn.commit();
                        Thread.sleep(1); // Avoid busy loop.
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("Should not fail to commit an empty transaction.");
                        throw new RuntimeException("Should not fail to commit an empty transaction.");
                    }
                    // Start a new transaction and wait again.
                    pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, ApiaryConfig.systemRole, pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
                }
            }
            long t2 = System.nanoTime();
            prepareTxnTimes.add(t2 - t1);
            // Finally, launch this transaction but does not wait.
            pgRpTask.replayTxnID = pgCtxt.txc.txID;
            pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(pgCtxt, pgRpTask));
            pendingCommitTasks.put(resTxId, pgRpTask);
            long t3 = System.nanoTime();
            submitTimes.add(t3 - t2);
            totalTimes.add(t3 - t0);
        }
        startOrderRs.close();
        startOrderStmt.close();

        if (!pendingCommitTasks.isEmpty()) {
            Map<Long, Future<Long>> cleanUpTxns = new HashMap<>();
            // Commit the rest.
            // The order doesn't matter because if they could commit originally, they must have no conflicts.
            // If they didn't commit originally, then the order also doesn't matter.
            long t1 = System.nanoTime();
            for (long cmtTxn : pendingCommitTasks.keySet()) {
                PostgresReplayTask commitPgRpTask = pendingCommitTasks.get(cmtTxn);
                if (commitPgRpTask == null) {
                    logger.error("No task found for pending commit txn {}.", cmtTxn);
                    throw new RuntimeException("Failed to find commit transaction " + cmtTxn);
                }
                logger.debug("Commit final pending txnID {}.", cmtTxn);
                Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(commitPgRpTask, workerContext, cmtTxn, replayMode, threadPool));
                cleanUpTxns.put(cmtTxn, cmtFut);

            }

            for (long cmtTxn : cleanUpTxns.keySet()) {
                try {
                    long retVal = cleanUpTxns.get(cmtTxn).get(5, TimeUnit.SECONDS);
                    if (retVal != cmtTxn) {
                        logger.error("Final pending commit failed. Commit txn: {}, retVal: {}", cmtTxn, retVal);
                    }
                } catch (TimeoutException e) {
                    logger.error("Final pending commit timed out for transaction {}", cmtTxn);
                }
                connPool.add(pendingCommitTasks.get(cmtTxn).conn);
                pendingCommitTasks.remove(cmtTxn);
            }
            long t2 = System.nanoTime();
            commitTimes.add(t2 - t1);
        }

        if (!pendingTasks.isEmpty() && workerContext.hasRetroFunctions()) {
            // Execute everything left to be processed, only in retro mode.
            // TODO: a better way to execute new tasks?
            logger.info("Process unfinished tasks.");
            Connection currConn = connPool.poll();
            for (long execId : pendingTasks.keySet()) {
                Map<Long, Task> execFuncs = pendingTasks.get(execId);
                while (!execFuncs.isEmpty()) {
                    for (long funcId : execFuncs.keySet()) {
                        totalStartOrderTxns++;
                        Task rpTask = execFuncs.get(funcId);
                        PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);
                        PostgresContext pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, ApiaryConfig.systemRole, pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
                        pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(pgCtxt, pgRpTask));
                        Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(pgRpTask, workerContext, pgCtxt.txc.txID, replayMode, threadPool));
                        cmtFut.get(5, TimeUnit.SECONDS);
                    }
                }
            }
            connPool.add(currConn);
        }
        long elapsedTime = System.currentTimeMillis() - prepTime;

        logger.debug("Last non skipped execId: {}", lastNonSkippedExecId);
        Object output = execIdToFinalOutput.get(lastNonSkippedExecId);  // The last non-skipped execution ID.
        String outputString = output.toString();
        if (output instanceof int[]) {
            outputString = Arrays.toString((int[]) output);
        } else if (output instanceof String[]){
            outputString = Arrays.toString((String[]) output);
        }
        logger.debug("Final output: {} ...", outputString.substring(0, Math.min(outputString.length(), 100)));
        logger.info("Re-execution time: {} ms", elapsedTime);
        logger.info("Total original transactions: {}, re-executed transactions: {}", totalStartOrderTxns, totalExecTxns);
        printStats("Commit", commitTimes, elapsedTime);
        printStats("PrepareTxn", prepareTxnTimes, elapsedTime);
        printStats("SubmitTask", submitTimes, elapsedTime);
        printStats("TotalLoop", totalTimes, elapsedTime);

        // Clean up connection pool and statements.
        int totalNumConns = 0;
        while (!connPool.isEmpty()) {
            Connection currConn = connPool.poll();
            if (currConn != null) {
                currConn.close();
                totalNumConns++;
            }
        }
        logger.info("Total used connections: {}", totalNumConns);

        stmt.close();
        inputRs.close();
        inputStmt.close();
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
        commitThreadPool.shutdown();
        commitThreadPool.awaitTermination(10, TimeUnit.SECONDS);
        provConn.close();
        provInputConn.close();
        return output;
    }

    // Return the set of functions that cannot be skipped.
    private static Set<String> getExecFuncSets(WorkerContext workerContext) {
        Set<String> allFunctions = new HashSet<>(workerContext.listAllFunctions());
        int totalNumFuncs = allFunctions.size();
        Set<String> allFunctionSets = workerContext.listAllFunctionSets();
        Set<String> execFuncs = new HashSet<>();
        Set<String> restFunctions = new HashSet<>(); // The rest of non-read-only functions.
        Set<String> readWriteSet = new HashSet<>();
        Set<String> writeSet = new HashSet<>();

        // All retroactively modified functions and their function sets must be included.
        for (String funcSetName : allFunctionSets) {
            List<String> funcSet = workerContext.getFunctionSet(funcSetName);
            boolean hasRetro = false;
            for (String funcName : funcSet) {
                if (workerContext.retroFunctionExists(funcName)) {
                    logger.debug("{} function set contains retro modified function {}, cannot skip.", funcSetName, funcName);
                    hasRetro = true;
                }
            }
            if (hasRetro) {
                execFuncs.addAll(funcSet);
            } else {
                // If does not contain retro functions, only need to conisder non-read-only ones.
                if (!workerContext.getFunctionSetReadOnly(funcSetName)) {
                    restFunctions.addAll(funcSet);
                } else {
                    logger.debug("Skip read-only function set {} - {}", funcSetName, funcSet.toString());
                }
            }
        }
        // Remove functions that are already in the lists, check again.
        allFunctions.removeAll(execFuncs);
        allFunctions.removeAll(restFunctions);
        for (String funcName : allFunctions) {
            if (workerContext.retroFunctionExists(funcName)) {
                logger.debug("Retro modified function {}, cannot skip.", funcName);
                execFuncs.add(funcName);
            } else {
                if (!workerContext.getFunctionReadOnly(funcName)) {
                    restFunctions.add(funcName);
                } else {
                    logger.debug("Skip read-only function {}", funcName);
                }
            }
        }

        // Compute initial read set and write set.
        for (String funcName : execFuncs) {
            writeSet.addAll(workerContext.getFunctionSetTables(funcName, false));
            readWriteSet.addAll(workerContext.getFunctionSetTables(funcName, true));
            readWriteSet.addAll(writeSet);
        }

        // Compute a transitive closure.
        while (execFuncs.size() < totalNumFuncs) {
            Set<String> newFuncs = new HashSet<>();

            // Check per function set. Because we need to execute a request.
            for (String firstFuncName : restFunctions) {
                if (newFuncs.contains(firstFuncName)) {
                    continue;
                }
                List<String> funcSet = workerContext.getFunctionSet(firstFuncName);
                for (String funcName : funcSet) {
                    Set<String> funcWriteSet = new HashSet<>(workerContext.getFunctionSetTables(funcName, false));
                    Set<String> funcReadSet = new HashSet<>(workerContext.getFunctionSetTables(funcName, true));
                    if (funcWriteSet.isEmpty() && funcReadSet.isEmpty()) {
                        // Conservatively cannot skip.
                        newFuncs.addAll(funcSet);
                        logger.debug("Function {} read/write set empty. Cannot skip function set {}.", funcName, firstFuncName);
                        break;
                    }

                    // Check if their write sets intersect with the read or write sets of re-executed requests, or if their read sets intersect with the write sets of re-executed requests.
                    funcWriteSet.retainAll(readWriteSet);
                    funcReadSet.retainAll(writeSet);
                    if (!funcWriteSet.isEmpty() || !funcReadSet.isEmpty()) {
                        // Intersection not empty. Must be re-executed.
                        newFuncs.addAll(funcSet);
                        logger.debug("Function {} RW set overlap. ReadSet {}, WriteSet {}. Cannot skip function set {}", funcName, funcReadSet.toString(), funcWriteSet.toString(), firstFuncName);
                        break;
                    }
                }
            }
            if (newFuncs.isEmpty()) {
                break; // Stop if no new function added.
            }
            execFuncs.addAll(newFuncs);
            for (String newFunc : newFuncs) {
                readWriteSet.addAll(workerContext.getFunctionSetTables(newFunc, false));
                readWriteSet.addAll(workerContext.getFunctionSetTables(newFunc, true));
                writeSet.addAll(workerContext.getFunctionSetTables(newFunc, false));
            }
            restFunctions.removeAll(newFuncs);
        }
        logger.info("Selective replay must execute these functions: {}", execFuncs.toString());
        return execFuncs;
    }

    private static void printStats(String opName, Collection<Long> rawTimes, long elapsedTime) {
        List<Long> queryTimes = rawTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.debug("[{}]: Duration: {}  Operations: {} Op/sec: {} Average: {}μs p50: {}μs p99: {}μs", opName, elapsedTime, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.debug("No {}.", opName);
        }
    }

}
