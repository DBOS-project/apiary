package org.dbos.apiary.postgres;

import org.dbos.apiary.function.Task;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PostgresCommitCallable implements Callable<Long> {
    private static final Logger logger = LoggerFactory.getLogger(PostgresCommitCallable.class);

    private final PostgresReplayTask commitPgRpTask;
    private final WorkerContext workerContext;
    private final long cmtTxn;
    private final int replayMode;
    private final ExecutorService threadPool;

    public PostgresCommitCallable(PostgresReplayTask commitPgRpTask, WorkerContext workerContext, long cmtTxn, int replayMode, ExecutorService threadPool) {
        this.commitPgRpTask = commitPgRpTask;
        this.workerContext = workerContext;
        this.cmtTxn = cmtTxn;
        this.replayMode = replayMode;
        this.threadPool = threadPool;
    }

    // Process a commit, return transaction ID on success, -1 on failure.
    @Override
    public Long call() {
        try {
            // Wait for the task to finish and commit.
            int res = commitPgRpTask.resFut.get(5, TimeUnit.SECONDS);
            if (res == 0) {
                if (commitPgRpTask.fo.errorMsg.isEmpty()){
                    if (!workerContext.getFunctionReadOnly(commitPgRpTask.task.funcName)) {
                        // Only commit transactions with writes here.
                        commitPgRpTask.conn.commit();
                    } else {
                        logger.debug("Skip dependent read-only transaction {} -- should have been committed. ", cmtTxn);
                    }
                } else {
                    logger.debug("Skip commit {} due to Error message: {}", cmtTxn, commitPgRpTask.fo.errorMsg);
                }
            } else if (res == -1) {
                try {
                    commitPgRpTask.conn.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Should not fail to commit an empty transaction.");
                    throw new RuntimeException("Should not fail to commit an empty transaction.");
                }
                logger.debug("Replayed task failed or skipped for txn {}. result: {}", cmtTxn, res);
            } else {
                logger.debug("Replayed task failed inside transaction {}. result: {}", cmtTxn, res);
            }
        } catch (Exception e) {
            // Retry the pending commit function if it's a serialization error.
            // Note: this should only happen during retroactive programming. Because normal replay should only replay originally committed transactions.
            if (e instanceof PSQLException) {
                PSQLException p = (PSQLException) e;
                logger.debug("PSQLException during replay transaction {}: {}", cmtTxn, p.getMessage());
                if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState()) && workerContext.hasRetroFunctions()) {
                    logger.debug("Retry transaction {} due to serilization error. ", cmtTxn);
                    try {
                        commitPgRpTask.conn.rollback();
                        logger.debug("Rolled back failed to commit transaction {}.", cmtTxn);
                        PostgresContext pgCtxt = new PostgresContext(commitPgRpTask.conn, workerContext, ApiaryConfig.systemRole, commitPgRpTask.task.execId, commitPgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
                        commitPgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(pgCtxt, commitPgRpTask));
                        commitPgRpTask.resFut.get(5, TimeUnit.SECONDS);
                        commitPgRpTask.conn.commit();
                        logger.debug("Committed retried PSQLException transaction {}.", cmtTxn);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        logger.error("Unrecoverable error during retry transaction {}. Skipped. Error message: {}", cmtTxn, ex.getMessage());
                    }
                } else {
                    logger.error("Unrecoverable error. Failed to commit {}, skipped. Error message: {}", cmtTxn, p.getMessage());
                }
            } else if (e instanceof TimeoutException) {
                // Timeout due to blocking (write conflicts), has to terminate it.
                // TODO: maybe support retry?
                logger.error("Transaction {} time out, may due to write conflicts.", cmtTxn);
                try {
                    commitPgRpTask.conn.abort(Runnable::run);
                    logger.debug("Rolled back timeout transaction");
                    commitPgRpTask.conn = workerContext.getPrimaryConnection().createNewConnection();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new RuntimeException("Unrecoverable error during aborting timed out transaction.");
                }
            } else {
                logger.error("Other failures during replay transaction {}, cannot commit: {} - {}", cmtTxn, e.getClass().getName(), e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Unrecoverable error during commit.");
            }
        }
        return cmtTxn;
    }

}
