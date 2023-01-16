package org.dbos.apiary.postgres;

import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.worker.ReplayTask;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PostgresRetroReplay {
    private static final Logger logger = LoggerFactory.getLogger(PostgresRetroReplay.class);

    public static Object retroExecuteAll(WorkerContext workerContext, long targetExecID, int replayMode) throws Exception {
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            logger.debug("Replay the entire trace!");
        } else if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
            logger.debug("Selective replay!");
        } else {
            logger.error("Do not support replay mode: {}", replayMode);
            return null;
        }
        assert(workerContext.provBuff != null);
        Connection provConn = workerContext.provBuff.conn.get();

        // Find previous execution history, only execute later committed transactions.
        // TODO: should we re-execute aborted transaction (non-recoverable failures), especially for bug reproduction?
        String provQuery = String.format("SELECT %s, %s FROM %s WHERE %s = %d AND %s=0 AND %s=0 AND %s=\'%s\';",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations,
                ProvenanceBuffer.PROV_EXECUTIONID, targetExecID, ProvenanceBuffer.PROV_FUNCID,
                ProvenanceBuffer.PROV_ISREPLAY, ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT);
        Statement stmt = provConn.createStatement();
        ResultSet historyRs = stmt.executeQuery(provQuery);
        long origTxid = -1;
        if (historyRs.next()) {
            origTxid = historyRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        } else {
            logger.error("No corresponding original transaction for execution {}", targetExecID);
            throw new RuntimeException("Cannot find original transaction!");
        }
        historyRs.close();

        // Replay based on the snapshot info, because transaction/commit order != actual serial order.
        // Start transactions based on their original txid order, but commit based on commit order.
        // Maintain a pool of connections to the backend database to concurrently execute transactions.

        // This query finds the starting order of transactions.
        String startOrderQuery = String.format("SELECT * FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        Statement startOrderStmt = provConn.createStatement();
        ResultSet startOrderRs = startOrderStmt.executeQuery(startOrderQuery);
        if (!startOrderRs.next()) {
            logger.error("Cannot find start order with query: {}", startOrderQuery);
            return null;
        }

        // This query finds the commit order of transactions.
        String commitOrderQuery = String.format("SELECT %s, %s FROM %s WHERE %s >= %d AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, origTxid,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_END_TIMESTAMP);
        Statement commitOrderStmt = provConn.createStatement();
        ResultSet commitOrderRs = commitOrderStmt.executeQuery(commitOrderQuery);
        // Next commit transaction ID, the next to be committed.
        if (!commitOrderRs.next()) {
            logger.error("Cannot find commit order with query: {}", commitOrderQuery);
            return null;
        }
        long nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);

        // This query finds the original input.
        String inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                        "WHERE %s >= %d AND %s = 0 AND %s = 0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                origTxid, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
        );
        Statement inputStmt = provConn.createStatement();
        ResultSet inputRs = inputStmt.executeQuery(inputQuery);

        // Cache inputs of the original execution. <execId, input>
        long currInputExecId = -1;
        Object[] currInputs = null;

        // Store currently unresolved tasks. <execId, funcId, task>
        Map<Long, Map<Long, Task>> pendingTasks = new HashMap<>();

        // Store funcID to value mapping of each execution.
        Map<Long, Map<Long, Object>> execFuncIdToValue = new HashMap<>();

        // Store execID to final output map. Because the output could be a future.
        // TODO: garbage collect this map.
        Map<Long, Object> execIdToFinalOutput = new HashMap<>();

        // Store a list of skipped requests. Used for selective replay.
        Set<Long> skippedExecIds = new HashSet<>();
        long lastNonSkippedExecId = -1;  // The last not-skipped execution ID. Useful to decide the final output.

        // A connection pool to the backend database. For concurrent executions.
        int connPoolSize = 10;  // Connection pool size. TODO: tune this.
        Queue<Connection> connPool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < connPoolSize; i++) {
            connPool.add(workerContext.getPrimaryConnection().createNewConnection());
        }

        // A pending commit map from transaction ID to connection. <txid, connection>
        Map<Long, Connection> pendingCommits = new HashMap<>();
        // A map storing the task info for a pending commit transaction. GC after the task is committed.
        Map<Long, ReplayTask> pendingCommitTask = new HashMap<>();

        while (nextCommitTxid > 0) {
            // Execute all following functions until nextCommitTxid is in the snapshot of that original transaction.
            // If the nextCommitTxid is in the snapshot, then that function needs to start after it commits.
            while (true) {
                long resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
                long resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
                long resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
                String[] resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
                String resName = resNames[resNames.length - 1]; // Extract the actual function name.
                String resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
                long xmax = PostgresUtilities.parseXmax(resSnapshotStr);
                List<Long> activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);

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

                    // TODO: Can we skip empty transactions?
                    ReplayTask rpTask = new ReplayTask(resExecId, resFuncId, resName, currInputs);

                    // Check if we can skip this function execution. If so, add to the skip list. Otherwise, execute the replay.

                    boolean isSkipped = checkSkipFunc(workerContext, rpTask, skippedExecIds, replayMode);

                    if (isSkipped) {
                        logger.debug("Skipping transaction {}, execution ID {}", resTxId, resExecId);
                        skippedExecIds.add(resExecId);
                    } else {
                        lastNonSkippedExecId = resExecId;
                        pendingCommitTask.put(resTxId, rpTask);
                        Connection currConn = connPool.poll();
                        if (currConn == null) {
                            throw new RuntimeException("Not enough connections to replay!");
                        }

                        // Execute the function.
                        processReplayFunction(workerContext, currConn, rpTask, replayMode, pendingTasks,
                                execFuncIdToValue, execIdToFinalOutput);
                        pendingCommits.put(resTxId, currConn);
                    }
                } else {
                    break;  // Need to wait until nextCommitTxid to commit.
                }

                if (!startOrderRs.next()) {
                    // No more to process.
                    break;
                }
            }

            // Commit the nextCommitTxid and update the variables. Pass skipped functions.
            // The connection must be not null because it has to have started.
            Connection commitConn = pendingCommits.get(nextCommitTxid);

            // If commitConn is null, then the transaction was skipped.
            if (commitConn == null) {
                logger.debug("Transaction {} was skipped. No connection found.", nextCommitTxid);
            } else {
                try {
                    commitConn.commit();
                } catch (Exception e) {
                    // Retry the pending commit function if it's a serialization error.
                    // Note: this should only happen during retroactive programming. Because normal replay should only replay originally committed transactions.
                    if (e instanceof PSQLException) {
                        PSQLException p = (PSQLException) e;
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                            logger.debug("Retry transaction {} due to serilization error. ", nextCommitTxid);
                            try {
                                commitConn.rollback();
                                logger.debug("Rolled back failed to commit transaction.");
                                ReplayTask rt = pendingCommitTask.get(nextCommitTxid);
                                assert (rt != null);
                                processReplayFunction(workerContext, commitConn, rt, replayMode, pendingTasks,
                                        execFuncIdToValue, execIdToFinalOutput);
                                commitConn.commit();
                                logger.debug("Committed retried transaction.");
                            } catch (SQLException ex) {
                                ex.printStackTrace();
                            }
                        } else {
                            logger.error("Unrecoverable error. Failed to commit {}, skipped. Error message: {}", nextCommitTxid, e.getMessage());
                            throw new RuntimeException("Unrecoverable error during replay.");
                        }
                    }
                }
                // Put it back to the connection pool and delete stored inputs.
                connPool.add(commitConn);
                pendingCommits.remove(nextCommitTxid);
                pendingCommitTask.remove(nextCommitTxid);
            }

            if (commitOrderRs.next()) {
                nextCommitTxid = commitOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            } else {
                nextCommitTxid = 0;
            }
        }

        if (!pendingCommits.isEmpty()) {
            throw new RuntimeException("Still more pending transactions to be committed!");
        }

        if (!pendingCommitTask.isEmpty()) {
            throw new RuntimeException("Still more pending commit tasks not garbage collected!");
        }

        if (!pendingTasks.isEmpty()) {
            throw new RuntimeException("Still more pending tasks to be resolved! Currently do not support adding transactions.");
        }

        Object output = execIdToFinalOutput.get(lastNonSkippedExecId);  // The last non-skipped execution ID.

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
        return output;
    }

    // Return true if the function execution can be skipped.
    private static boolean checkSkipFunc(WorkerContext workerContext, ReplayTask rpTask, Set<Long> skippedExecIds, int replayMode) {
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            // Do not skip if we are replaying everything.
            return false;
        }

        // The current selective replay heuristic:
        // 1) If a request has been skipped, then all following functions will be skipped.
        // 2) If a function name is not in the list of retroFunctions, then we can skip. TODO: update this because a function may not be in retroFunctions but still need to be replayed. Need to use the write set to check.
        if (skippedExecIds.contains(rpTask.execId)) {
            return true;
        }

        if (!workerContext.retroFunctionExists(rpTask.funcName)) {
            return true;
        }

        return false;
    }

    // Return true if executed the function, false if nothing executed.
    private static boolean processReplayFunction(WorkerContext workerContext, Connection conn, ReplayTask task, int replayMode, Map<Long, Map<Long, Task>> pendingTasks,
                                          Map<Long, Map<Long, Object>> execFuncIdToValue,
                                          Map<Long, Object> execIdToFinalOutput) {
        FunctionOutput fo;
        // Only support primary functions.
        if (!workerContext.functionExists(task.funcName)) {
            logger.debug("Unrecognized function: {}, cannot replay, skipped.", task.funcName);
            return false;
        }
        String type = workerContext.getFunctionType(task.funcName);
        if (!workerContext.getPrimaryConnectionType().equals(type)) {
            logger.error("Replay only support primary functions!");
            throw new RuntimeException("Replay only support primary functions!");
        }

        PostgresConnection c = (PostgresConnection) workerContext.getPrimaryConnection();

        if (task.funcId == 0l) {
            // This is the first function of a request.
            fo = c.replayFunction(conn, task.funcName, workerContext, "retroReplay", task.execId, task.funcId,
                    replayMode, task.inputs);
            if (fo == null) {
                logger.warn("Replay function output is null.");
                return false;
            }
            execFuncIdToValue.putIfAbsent(task.execId, new HashMap<>());
            execIdToFinalOutput.putIfAbsent(task.execId, fo.output);
            pendingTasks.putIfAbsent(task.execId, new HashMap<>());
        } else {
            // Skip the task if it is absent. Because we allow reducing the number of called function
            // (currently does not support adding more).
            if (!pendingTasks.containsKey(task.execId) || !pendingTasks.get(task.execId).containsKey(task.funcId)) {
                logger.info("Skip function ID {}, not found in pending tasks.", task.funcId);
                return false;
            }
            // Find the task in the stash. Make sure that all futures have been resolved.
            Task currTask = pendingTasks.get(task.execId).get(task.funcId);

            // Resolve input for this task. Must success.
            Map<Long, Object> currFuncIdToValue = execFuncIdToValue.get(task.execId);

            if (!currTask.dereferenceFutures(currFuncIdToValue)) {
                logger.error("Failed to dereference input for execId {}, funcId {}. Aborted", task.execId, task.funcId);
                throw new RuntimeException("Retro replay failed to dereference input.");
            }

            fo = c.replayFunction(conn, currTask.funcName, workerContext,  "retroReplay", task.execId, task.funcId,
                    replayMode, currTask.input);
            // Remove this task from the map.
            pendingTasks.get(task.execId).remove(task.funcId);
            if (fo == null) {
                logger.warn("Repaly function output is null.");
                return false; // TODO: better error handling?
            }
        }
        // Store output value.
        execFuncIdToValue.get(task.execId).putIfAbsent(task.funcId, fo.output);
        // Queue all of its async tasks to the pending map.
        for (Task t : fo.queuedTasks) {
            if (pendingTasks.get(task.execId).containsKey(t.functionID)) {
                logger.error("ExecID {} funcID {} has duplicated outputs!", task.execId, t.functionID);
            }
            pendingTasks.get(task.execId).putIfAbsent(t.functionID, t);
        }

        if (pendingTasks.get(task.execId).isEmpty()) {
            // Check if we need to update the final output map.
            Object o = execIdToFinalOutput.get(task.execId);
            if (o instanceof ApiaryFuture) {
                ApiaryFuture futureOutput = (ApiaryFuture) o;
                assert (execFuncIdToValue.get(task.execId).containsKey(futureOutput.futureID));
                Object resFo = execFuncIdToValue.get(task.execId).get(futureOutput.futureID);
                execIdToFinalOutput.put(task.execId, resFo);
            }
            // Clean up.
            execFuncIdToValue.remove(task.execId);
            pendingTasks.remove(task.execId);
        }
        return true;
    }
}
