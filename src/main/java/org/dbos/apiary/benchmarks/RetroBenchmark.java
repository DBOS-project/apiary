package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.retro.MoodleBenchmark;
import org.dbos.apiary.benchmarks.tpcc.TPCCBenchmark;
import org.dbos.apiary.benchmarks.retro.WordPressBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RetroBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RetroBenchmark.class);

    public static int provenancePort = 5433;  // Change to 5433 for a separate Postgres or Vertica.
    public static String provenanceDB = ApiaryConfig.vertica; // Change to Vertica as needed.
    public static String provenanceAddr = "localhost"; // Change to other addresses as needed.

    public static void benchmark(String appName, String dbAddr, Integer interval, Integer duration, boolean skipLoad, int retroMode, long startExecId, long endExecId, String bugFix, List<Integer> percentages) throws Exception {

        // Disable provenance tracking, only capture func invocations and input.
        ApiaryConfig.captureReads = false;
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureMetadata = false;

        // Change provenance info.
        if (!ApiaryConfig.recordInput) {
            // Use postgres itself just to pass init.
            provenanceAddr = dbAddr;
            provenanceDB = ApiaryConfig.postgres;
            provenancePort = ApiaryConfig.postgresPort;
        }
        ApiaryConfig.provenancePort = provenancePort;

        if (appName.equalsIgnoreCase("moodle")) {
            MoodleBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        } else if (appName.equalsIgnoreCase("wordpress")) {
            WordPressBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        } else if (appName.equalsIgnoreCase("tpcc")) {
            TPCCBenchmark.benchmark(dbAddr, interval, duration, retroMode, startExecId, endExecId, percentages);
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
