package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.retro.MoodleBenchmark;
import org.dbos.apiary.benchmarks.retro.WordPressBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RetroBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RetroBenchmark.class);

    public static void benchmark(String appName, String dbAddr, Integer interval, Integer duration, boolean skipLoad, int retroMode, long startExecId, long endExecId, String bugFix, List<Integer> percentages) throws Exception {

        // Always disable read capture for retro replay mode. Only capture read metadata.
        ApiaryConfig.captureReads = false;
        if (appName.equalsIgnoreCase("moodle")) {
            MoodleBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        } else if (appName.equalsIgnoreCase("wordpress")) {
            WordPressBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        }
    }

    public static void retroReplayExec(ApiaryWorkerClient client, int replayMode, long startExecId, long endExecId) throws InvalidProtocolBufferException {
        if (replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
            // Replay a single execution.
            logger.warn("No support single replay for benchmark.");
        } else if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()){
            FunctionOutput res = client.retroReplay(startExecId, endExecId, ApiaryConfig.ReplayMode.ALL.getValue());
            assert (res != null);
        } else if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
            FunctionOutput res = client.retroReplay(startExecId, endExecId, ApiaryConfig.ReplayMode.SELECTIVE.getValue());
            assert (res != null);
        } else {
            logger.error("Do not support replay mode {}", replayMode);
        }
    }
}
