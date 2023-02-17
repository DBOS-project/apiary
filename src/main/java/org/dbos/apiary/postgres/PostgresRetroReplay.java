package org.dbos.apiary.postgres;

import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class PostgresRetroReplay {
    private static final Logger logger = LoggerFactory.getLogger(PostgresRetroReplay.class);

    public static Object retroExecuteAll(WorkerContext workerContext, long targetExecID, long endExecId, int replayMode) throws Exception {
        long startTime = System.currentTimeMillis();
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            logger.debug("Replay the entire trace!");
        } else if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
            logger.debug("Selective replay!");
        } else {
            logger.error("Do not support replay mode: {}", replayMode);
            return null;
        }
        assert(workerContext.provBuff != null);
        Connection provConn = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);

        // Record a list of modified tables. Track dependencies for replaying requests.
        Set<String> replayWrittenTables = ConcurrentHashMap.newKeySet();

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
        long origTxid = -1;
        if (historyRs.next()) {
            origTxid = historyRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay start transaction ID: {}", origTxid);
        } else {
            logger.error("No corresponding original transaction for start execution {}", targetExecID);
            throw new RuntimeException("Cannot find original transaction!");
        }
        historyRs.close();

        // Find the transaction ID of the last execution. Only need to find the transaction ID of the first function.
        // Execute everything between [origTxid, endTxId)
        stmt.setLong(1, endExecId);
        ResultSet endRs = stmt.executeQuery();
        long endTxId = Long.MAX_VALUE;
        if (endRs.next()) {
            endTxId = endRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay end transaction ID (excluded): {}", endTxId);
        } else {
            logger.debug("No corresponding original transaction for end execution {}. Execute the entire trace!", endExecId);
        }

        // Replay based on the snapshot info, because transaction/commit order != actual serial order.
        // Start transactions based on their original txid order, but commit based on commit order.
        // Maintain a pool of connections to the backend database to concurrently execute transactions.

        // This query finds the starting order of transactions.
        // Replay mode only consider committed transactions.
        String startOrderQuery = String.format("SELECT * FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        if (workerContext.hasRetroFunctions()) {
            // Include abroted transactions.
            startOrderQuery = String.format("SELECT * FROM %s WHERE %s >= %d AND %s=0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                    ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                    ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        }
        Statement startOrderStmt = provConn.createStatement();
        ResultSet startOrderRs = startOrderStmt.executeQuery(startOrderQuery);
        if (!startOrderRs.next()) {
            logger.error("Cannot find start order with query: {}", startOrderQuery);
            return null;
        }
        long resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
        String[] resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
        String resName = resNames[resNames.length - 1]; // Extract the actual function name.
        String resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
        String resStatus = startOrderRs.getString(ProvenanceBuffer.PROV_FUNC_STATUS);
        long xmax = PostgresUtilities.parseXmax(resSnapshotStr);
        List<Long> activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);


        // This query finds the commit order of transactions.
        String commitOrderQuery = String.format("SELECT %s, %s FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_END_TIMESTAMP);
        if (workerContext.hasRetroFunctions()) {
            commitOrderQuery = String.format("SELECT %s, %s FROM %s WHERE %s >= %d AND %s=0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                    ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                    ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                    ProvenanceBuffer.PROV_END_TIMESTAMP);
        }
        Connection provConn2 = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);

        Statement commitOrderStmt = provConn2.createStatement();
        ResultSet commitOrderRs = commitOrderStmt.executeQuery(commitOrderQuery);
        // Next commit transaction ID, the next to be committed.
        if (!commitOrderRs.next()) {
            logger.error("Cannot find commit order with query: {}", commitOrderQuery);
            return null;
        }
        long nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);

        // This query finds the original input.
        String inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                        "WHERE %s >= %d AND %s = 0 AND %s = 0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                origTxid, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
        );
        Connection provConn3 = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);
        Statement inputStmt = provConn3.createStatement();
        ResultSet inputRs = inputStmt.executeQuery(inputQuery);

        // Cache accessed tables of a function set. <firstFuncName, String[]>
        Map<String, String[]> funcSetAccessTables = new ConcurrentHashMap<>();

        // Cache inputs of the original execution. <execId, input>
        long currInputExecId = -1;
        Object[] currInputs = null;

        // Store currently unresolved tasks. <execId, funcId, task>
        Map<Long, Map<Long, Task>> pendingTasks = new ConcurrentHashMap<>();

        // Store funcID to value mapping of each execution.
        Map<Long, Map<Long, Object>> execFuncIdToValue = new ConcurrentHashMap<>();

        // Store execID to final output map. Because the output could be a future.
        // TODO: garbage collect this map.
        Map<Long, Object> execIdToFinalOutput = new ConcurrentHashMap<>();

        // Store a list of skipped requests. Used for selective replay.
        Set<Long> skippedExecIds = ConcurrentHashMap.newKeySet();
        long lastNonSkippedExecId = -1;  // The last not-skipped execution ID. Useful to decide the final output.

        // A connection pool to the backend database. For concurrent executions.
        Queue<Connection> connPool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < workerContext.numWorkersThreads; i++) {
            connPool.add(workerContext.getPrimaryConnection().createNewConnection());
        }

        // A pending commit map from original transaction ID to Postgres replay task.
        Map<Long, PostgresReplayTask> pendingCommitTasks = new ConcurrentHashMap<>();

        // A thread pool for concurrent function executions.
        ExecutorService threadPool = Executors.newFixedThreadPool(workerContext.numWorkersThreads);

        // Caches for committed transactions and aborted transactions.
        List<PostgresReplayTask> committedTasks = new ArrayList<>();
        List<PostgresReplayTask> abortedTasks = new ArrayList<>();

        long prepTime = System.currentTimeMillis();
        logger.info("Prepare time: {} ms", prepTime - startTime);
        boolean startHasNext = true;
        while ((nextCommitTxid > 0) && (nextCommitTxid < endTxId)) {
            // Execute all following functions until nextCommitTxid is in the snapshot of that original transaction.
            // If the nextCommitTxid is in the snapshot, then that function needs to start after it commits.
            while (startHasNext) {

                if ((resTxId == nextCommitTxid) || (nextCommitTxid >= xmax) || (activeTxns.contains(nextCommitTxid))) {
                    // Not in its snapshot. Start a new transaction.

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
                            logger.debug("Original arguments execid {}, inputs {}", currInputExecId, currInputs);
                        } else {
                            logger.error("Could not find the input for this execution ID {} ", resExecId);
                            throw new RuntimeException("Retro replay failed due to missing input.");
                        }
                    }

                    Task rpTask = new Task(resExecId, resFuncId, resName, currInputs);

                    // Check if we can skip this function execution. If so, add to the skip list. Otherwise, execute the replay.
                    boolean isSkipped = checkSkipFunc(workerContext, rpTask, skippedExecIds, replayMode, replayWrittenTables, funcSetAccessTables);

                    if (isSkipped && (lastNonSkippedExecId != -1)) {
                        // Do not skip the first execution.
                        logger.debug("Skipping transaction {}, execution ID {}", resTxId, resExecId);
                        skippedExecIds.add(resExecId);
                    } else {
                        lastNonSkippedExecId = resExecId;

                        Connection currConn = connPool.poll();
                        if (currConn == null) {
                            throw new RuntimeException("Not enough connections to replay!");
                        }

                        // Store in the cache based on their status.
                        PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);
                        if (resStatus.equals(ProvenanceBuffer.PROV_STATUS_COMMIT)) {
                            committedTasks.add(pgRpTask);
                        } else {
                            abortedTasks.add(pgRpTask);
                        }
                        pendingCommitTasks.put(resTxId, pgRpTask);
                    }
                } else {
                    break;  // Need to wait until nextCommitTxid to commit.
                }
                if (!startOrderRs.next()) {
                    // No more to process.
                    startHasNext = false;
                    break;
                }
                resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
                resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
                resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
                resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
                resName = resNames[resNames.length - 1]; // Extract the actual function name.
                resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
                resStatus = startOrderRs.getString(ProvenanceBuffer.PROV_FUNC_STATUS);
                xmax = PostgresUtilities.parseXmax(resSnapshotStr);
                activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);
            }

            // Launch committed tasks first, then aborted tasks.
            for (PostgresReplayTask pgRpTask : committedTasks) {
                pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(workerContext, pgRpTask, replayMode, pendingTasks,
                        execFuncIdToValue, execIdToFinalOutput, replayWrittenTables));
                // A replay without any modified functions should never be blocked, so we don't need to wait.
                // TODO: a minor issue is that submitted tasks are not guaranteed to start transaction in the same order. It does not affect correctness.
            }
            committedTasks.clear();

            // We should only have aborted tasks if under retro mode.
            for (PostgresReplayTask pgRpTask : abortedTasks) {
                pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(workerContext, pgRpTask, replayMode, pendingTasks,
                        execFuncIdToValue, execIdToFinalOutput, replayWrittenTables));
            }
            abortedTasks.clear();

            // Commit the nextCommitTxid and update the variables. Pass skipped functions.
            // The connection must be not null because it has to have started.
            PostgresReplayTask commitPgRpTask = pendingCommitTasks.get(nextCommitTxid);
            logger.debug("Processing commit txid: {}", nextCommitTxid);
            // If commitConn is null, then the transaction was skipped.
            if (commitPgRpTask == null) {
                logger.debug("Transaction {} was skipped. No connection found.", nextCommitTxid);
            } else {
                try {
                    // Wait for the task to finish.
                    int res = commitPgRpTask.resFut.get(100, TimeUnit.MILLISECONDS);
                    if (res == 0) {
                        if (commitPgRpTask.fo.errorMsg.isEmpty()) {
                            commitPgRpTask.conn.commit();
                        } else {
                            logger.debug("Skip commit {} due to Error message: {}", nextCommitTxid, commitPgRpTask.fo.errorMsg);
                        }
                    } else {
                        logger.debug("Replayed task failed or skipped for transaction {}. result: {}", nextCommitTxid, res);
                    }
                } catch (Exception e) {
                    // Retry the pending commit function if it's a serialization error.
                    // Note: this should only happen during retroactive programming. Because normal replay should only replay originally committed transactions.
                    if (e instanceof PSQLException) {
                        PSQLException p = (PSQLException) e;
                        logger.debug("PSQLException during replay transaction {}: {}", nextCommitTxid, p.getMessage());
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState()) && workerContext.hasRetroFunctions()) {
                            logger.debug("Retry transaction {} due to serilization error. ", nextCommitTxid);
                            try {
                                commitPgRpTask.conn.rollback();
                                logger.debug("Rolled back failed to commit transaction.");
                                commitPgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(workerContext, commitPgRpTask, replayMode, pendingTasks,
                                        execFuncIdToValue, execIdToFinalOutput, replayWrittenTables));
                                commitPgRpTask.resFut.get(100, TimeUnit.MILLISECONDS);
                                commitPgRpTask.conn.commit();
                                logger.debug("Committed retried PSQLException transaction.");
                            } catch (Exception ex) {
                                ex.printStackTrace();
                                throw new RuntimeException("Unrecoverable error during retry.");
                            }
                        } else {
                            logger.error("Unrecoverable error. Failed to commit {}, skipped. Error message: {}", nextCommitTxid, e.getMessage());
                            throw new RuntimeException("Unrecoverable error during replay.");
                        }
                    } else if (e instanceof  TimeoutException) {
                        // Timeout due to blocking (write conflicts), has to terminate it.
                        // TODO: maybe support retry?
                        logger.debug("Transaction {} time out, may due to write conflicts.", nextCommitTxid);
                        try {
                            commitPgRpTask.conn.abort(Runnable::run);
                            logger.debug("Rolled back timeout transaction");
                            commitPgRpTask.conn = workerContext.getPrimaryConnection().createNewConnection();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            throw new RuntimeException("Unrecoverable error during aborting timed out transaction.");
                        }
                    } else {
                        logger.debug("Other failures during replay transaction {}, cannot commit: {} - {}", nextCommitTxid, e.getClass().getName(), e.getMessage());
                        // TODO: should we continue here? If we get a timeout exception, the transaction is blocked (shouldn't happen in replay but retro mode may introduce new deadlocks).
                        throw new RuntimeException("Unrecoverable error during commit.");
                    }
                }
                // Put it back to the connection pool and delete stored inputs.
                connPool.add(commitPgRpTask.conn);
                pendingCommitTasks.remove(nextCommitTxid);
            }

            if (commitOrderRs.next()) {
                nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            } else {
                nextCommitTxid = 0;
            }
        }

        if (!pendingCommitTasks.isEmpty()) {
            throw new RuntimeException("Still more pending transactions to be committed!");
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
                        Task rpTask = execFuncs.get(funcId);
                        PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);
                        pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(workerContext, pgRpTask, replayMode, pendingTasks,
                                execFuncIdToValue, execIdToFinalOutput, replayWrittenTables));
                        try {
                            int res = pgRpTask.resFut.get(100, TimeUnit.MILLISECONDS);
                            if (res == 0) {
                                if (pgRpTask.fo.errorMsg.isEmpty()) {
                                    currConn.commit();
                                } else {
                                    logger.debug("Skip commit {} due to Error message: {}", nextCommitTxid, pgRpTask.fo.errorMsg);
                                }
                            } else {
                                logger.debug("Replayed task failed or skipped for transaction {}. result: {}", nextCommitTxid, res);
                            }
                        } catch (Exception e) {
                            logger.debug("Other failures during replay transaction {}, cannot commit: {} - {}", nextCommitTxid, e.getClass().getName(), e.getMessage());
                            throw new RuntimeException("Unrecoverable error during commit.");
                        }
                    }
                }
            }
            connPool.add(currConn);
        }

        Object output = execIdToFinalOutput.get(lastNonSkippedExecId);  // The last non-skipped execution ID.

        long endTime = System.currentTimeMillis();
        logger.info("Re-execution time: {} ms", endTime - prepTime);

        // Clean up connection pool and statements.
        while (!connPool.isEmpty()) {
            Connection currConn = connPool.poll();
            if (currConn != null) {
                currConn.close();
            }
        }

        startOrderRs.close();
        startOrderStmt.close();
        stmt.close();
        inputRs.close();
        inputStmt.close();
        commitOrderRs.close();
        commitOrderStmt.close();
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
        provConn.close();
        provConn2.close();
        provConn3.close();
        return output;
    }

    // Return true if the function execution can be skipped.
    private static boolean checkSkipFunc(WorkerContext workerContext, Task rpTask, Set<Long> skippedExecIds, int replayMode, Set<String> replayWrittenTables, Map<String, String[]> funcSetAccessTables) throws SQLException {
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            // Do not skip if we are replaying everything.
            return false;
        }

        logger.debug("Replay written tables: {}", replayWrittenTables.toString());

        // The current selective replay heuristic:
        // 1) If a request has been skipped, then all following functions will be skipped.
        // 2) If a function name is in the list of retroFunctions, then we cannot skip.
        // 3) Cannot skip a request if any of its function contains writes and touches write set.
        if (skippedExecIds.contains(rpTask.execId)) {
            return true;
        } else if (rpTask.functionID > 0) {
            // If a request wasn't skipped at the first function, then the following functions cannot be skipped as well.
            // Reduce the number of checks.
            return false;
        }

        // Always replay a request if it contains modified functions.
        List<String> funcSet = workerContext.getFunctionSet(rpTask.funcName);
        if (funcSet == null) {
            // Conservatively, replay it.
            logger.debug("Does not find function set info, cannot skip.");
            return false;
        }
        for (String funcName : funcSet) {
            if (workerContext.retroFunctionExists(funcName)) {
                logger.debug("Contains retro modified function {}, cannot skip.", funcName);
                return false;
            }
        }


        // Check if an execution contains any writes, check all downstream functions.
        boolean isReadOnly = workerContext.getFunctionSetReadOnly(rpTask.funcName);
        logger.debug("Function set {} isReadonly? {}", rpTask.funcName, isReadOnly);
        if (!isReadOnly) {
            // If a request contains write but has nothing to do with the related table, we can skip it.
            // Check query metadata table and see if any transaction related to this execution touches any written tables.
            String[] tables;
            if (funcSetAccessTables.containsKey(rpTask.funcName)) {
                tables = funcSetAccessTables.get(rpTask.funcName);
            } else {
                tables = workerContext.getFunctionSetAccessTables(rpTask.funcName);
                funcSetAccessTables.put(rpTask.funcName, tables);
            }
            if (tables.length < 1) {
                // Conservatively, cannot skip.
                return false;
            }
            for (String table : tables) {
                if (replayWrittenTables.contains(table)) {
                    logger.debug("Execution would touch table {} in the write set. Cannot skip.", table);
                    return false;
                }
            }

        }

        return true;
    }

}
