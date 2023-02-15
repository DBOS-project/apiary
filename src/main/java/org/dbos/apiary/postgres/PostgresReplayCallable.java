package org.dbos.apiary.postgres;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static org.dbos.apiary.postgres.PostgresRetroReplay.*;

class PostgresReplayCallable implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(PostgresReplayCallable.class);


    private final PostgresReplayTask rpTask;
    private final PostgresContext pgCtxt;

    public PostgresReplayCallable(PostgresContext pgCtxt, PostgresReplayTask rpTask) {
        this.pgCtxt = pgCtxt;
        this.rpTask = rpTask;
    }

    // Process a replay function/transaction, and resolve dependencies.
    // Return 0 on success, -1 on failure outside of transaction, -2 on failure inside transaction. Store actual function output in rpTask.
    @Override
    public Integer call() {
        // Only support primary functions.
        if (!pgCtxt.workerContext.functionExists(rpTask.task.funcName)) {
            logger.debug("Unrecognized function: {}, cannot replay, skipped.", rpTask.task.funcName);
            return -1;
        }
        String type = pgCtxt.workerContext.getFunctionType(rpTask.task.funcName);
        if (!pgCtxt.workerContext.getPrimaryConnectionType().equals(type)) {
            logger.error("Replay only support primary functions!");
            return -1;
        }

        PostgresConnection c = (PostgresConnection) pgCtxt.workerContext.getPrimaryConnection();

        if (rpTask.task.functionID == 0L) {
            // This is the first function of a request.
            rpTask.fo = c.replayFunction(pgCtxt, rpTask.task.funcName, replayWrittenTables, rpTask.task.input);
            if (rpTask.fo == null) {
                logger.warn("Replay function output is null.");
                return -2;
            }
            // If contains error, then directly return.
            if ((rpTask.fo.errorMsg != null) && !rpTask.fo.errorMsg.isEmpty()) {
                logger.warn("Error message from replay: {}", rpTask.fo.errorMsg);
                return -2;
            }
            execFuncIdToValue.putIfAbsent(rpTask.task.execId, new ConcurrentHashMap<>());
            execIdToFinalOutput.putIfAbsent(rpTask.task.execId, rpTask.fo.output);
            pendingTasks.putIfAbsent(rpTask.task.execId, new ConcurrentHashMap<>());
        } else {
            // Find the task in the stash. Make sure that all futures have been resolved.
            Task currTask = pendingTasks.get(rpTask.task.execId).get(rpTask.task.functionID);

            // Resolve input for this task. Must success.
            Map<Long, Object> currFuncIdToValue = execFuncIdToValue.get(rpTask.task.execId);

            if (!currTask.dereferenceFutures(currFuncIdToValue)) {
                logger.error("Failed to dereference input for execId {}, funcId {}. Aborted", rpTask.task.execId, rpTask.task.functionID);
                return -1;
            }

            rpTask.fo = c.replayFunction(pgCtxt, currTask.funcName, replayWrittenTables, currTask.input);
            // Remove this task from the map.
            pendingTasks.get(rpTask.task.execId).remove(rpTask.task.functionID);
            if (rpTask.fo == null) {
                logger.warn("Replay function output is null.");
                return -2;
            }
        }

        // Store output value.
        execFuncIdToValue.get(rpTask.task.execId).putIfAbsent(rpTask.task.functionID, rpTask.fo.output);
        // Queue all of its async tasks to the pending map.
        for (Task t : rpTask.fo.queuedTasks) {
            if (pendingTasks.get(rpTask.task.execId).containsKey(t.functionID)) {
                logger.error("ExecID {} funcID {} has duplicated outputs!", rpTask.task.execId, t.functionID);
            }
            pendingTasks.get(rpTask.task.execId).putIfAbsent(t.functionID, t);
        }

        if (pendingTasks.get(rpTask.task.execId).isEmpty()) {
            // Check if we need to update the final output map.
            Object o = execIdToFinalOutput.get(rpTask.task.execId);
            if (o instanceof ApiaryFuture) {
                ApiaryFuture futureOutput = (ApiaryFuture) o;
                assert (execFuncIdToValue.get(rpTask.task.execId).containsKey(futureOutput.futureID));
                Object resFo = execFuncIdToValue.get(rpTask.task.execId).get(futureOutput.futureID);
                execIdToFinalOutput.put(rpTask.task.execId, resFo);
            }
            // Clean up.
            execFuncIdToValue.remove(rpTask.task.execId);
            pendingTasks.remove(rpTask.task.execId);
        }

        return 0;
    }

}
