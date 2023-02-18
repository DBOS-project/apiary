package org.dbos.apiary.postgres;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.Task;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class PostgresReplayCallable implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(PostgresReplayCallable.class);

    private final WorkerContext workerContext;
    private final PostgresReplayTask rpTask;
    private final int replayMode;
    private final Map<Long, Map<Long, Task>> pendingTasks;
    private final Map<Long, Map<Long, Object>> execFuncIdToValue;
    private final Map<Long, Object> execIdToFinalOutput;
    private final Set<String> replayWrittenTables;

    public PostgresReplayCallable(WorkerContext workerContext, PostgresReplayTask rpTask, int replayMode,
                                  Map<Long, Map<Long, Task>> pendingTasks,
                                  Map<Long, Map<Long, Object>> execFuncIdToValue,
                                  Map<Long, Object> execIdToFinalOutput,
                                  Set<String> replayWrittenTables) {
        this.workerContext = workerContext;
        this.rpTask = rpTask;
        this.replayMode = replayMode;
        this.pendingTasks = pendingTasks;
        this.execFuncIdToValue = execFuncIdToValue;
        this.execIdToFinalOutput = execIdToFinalOutput;
        this.replayWrittenTables = replayWrittenTables;
    }

    // Process a replay function/transaction, and resolve dependencies.
    // Return 0 on success, -1 on failure. Store actual function output in rpTask.
    @Override
    public Integer call() {
        // Only support primary functions.
        if (!workerContext.functionExists(rpTask.task.funcName)) {
            logger.debug("Unrecognized function: {}, cannot replay, skipped.", rpTask.task.funcName);
            return -1;
        }
        String type = workerContext.getFunctionType(rpTask.task.funcName);
        if (!workerContext.getPrimaryConnectionType().equals(type)) {
            logger.error("Replay only support primary functions!");
            return -1;
        }

        PostgresConnection c = (PostgresConnection) workerContext.getPrimaryConnection();

        if (rpTask.task.functionID == 0l) {
            // This is the first function of a request.
            rpTask.fo = c.replayFunction(rpTask.conn, rpTask.task.funcName, workerContext, "retroReplay", rpTask.task.execId, rpTask.task.functionID,
                    replayMode, replayWrittenTables, rpTask.task.input);
            if (rpTask.fo == null) {
                logger.warn("Replay function output is null.");
                return -1;
            }
            // If contains error, then directly return.
            if ((rpTask.fo.errorMsg != null) && !rpTask.fo.errorMsg.isEmpty()) {
                logger.warn("Error message from replay: {}", rpTask.fo.errorMsg);
                return -1;
            }
            execFuncIdToValue.putIfAbsent(rpTask.task.execId, new ConcurrentHashMap<>());
            execIdToFinalOutput.putIfAbsent(rpTask.task.execId, rpTask.fo.output);
            pendingTasks.putIfAbsent(rpTask.task.execId, new ConcurrentHashMap<>());
        } else {
            // Skip the task if it is absent. Because we allow reducing the number of called function
            if (!pendingTasks.containsKey(rpTask.task.execId)) {
                logger.error("Skip execution ID {}, function ID {}, not found in pending tasks.", rpTask.task.execId, rpTask.task.functionID);
                return -1;
            } else {
                // If execFuncIdToValue exists, then it means this task should have pending tasks, but due to concurrent execution, it's not written yet.
                if (!execFuncIdToValue.containsKey(rpTask.task.execId)) {
                    // Request has done.
                    logger.error("Skip execution ID {}, function ID {}, not found in pending tasks.", rpTask.task.execId, rpTask.task.functionID);
                    return -1;
                }
                Map<Long, Task> tmpTaskMap = null;
                while (execFuncIdToValue.containsKey(rpTask.task.execId)) {
                    // Busy spin, wait for its turn.
                    tmpTaskMap = pendingTasks.getOrDefault(rpTask.task.execId, Collections.emptyMap());
                    if (tmpTaskMap.containsKey(rpTask.task.functionID)) {
                        break;
                    }
                }
                if (tmpTaskMap == null) {
                    logger.error("Request done. Have to skip execution id {}, function id {}.", rpTask.task.execId, rpTask.task.functionID);
                    return -1;
                }
            }
            // Find the task in the stash. Make sure that all futures have been resolved.
            Task currTask = pendingTasks.get(rpTask.task.execId).get(rpTask.task.functionID);

            // Resolve input for this task. Must success.
            Map<Long, Object> currFuncIdToValue = execFuncIdToValue.get(rpTask.task.execId);

            if (!currTask.dereferenceFutures(currFuncIdToValue)) {
                logger.error("Failed to dereference input for execId {}, funcId {}. Aborted", rpTask.task.execId, rpTask.task.functionID);
                return -1;
            }

            rpTask.fo = c.replayFunction(rpTask.conn, currTask.funcName, workerContext, "retroReplay", rpTask.task.execId, rpTask.task.functionID,
                    replayMode, replayWrittenTables, currTask.input);
            // Remove this task from the map.
            pendingTasks.get(rpTask.task.execId).remove(rpTask.task.functionID);
            if (rpTask.fo == null) {
                logger.warn("Repaly function output is null.");
                return -1;
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
