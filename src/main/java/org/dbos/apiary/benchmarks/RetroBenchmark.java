package org.dbos.apiary.benchmarks;

import org.dbos.apiary.benchmarks.retro.MoodleBenchmark;
import org.dbos.apiary.benchmarks.retro.WordPressBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RetroBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RetroBenchmark.class);

    public static void benchmark(String appName, String dbAddr, Integer interval, Integer duration, boolean skipLoad, int retroMode, long startExecId, long endExecId, String bugFix, List<Integer> percentages) throws Exception {
        if (appName.equalsIgnoreCase("moodle")) {
            MoodleBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        } else if (appName.equalsIgnoreCase("wordpress")) {
            WordPressBenchmark.benchmark(dbAddr, interval, duration, skipLoad, retroMode, startExecId, endExecId, bugFix, percentages);
        }
    }
}
