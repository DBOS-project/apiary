package org.dbos.apiary.benchmarks;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BenchmarkingExecutable {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkingExecutable.class);

    public static boolean skipLoadData = false;

    // Ignore the illegal reflective access warning from VoltDB. TODO: Fix it later.
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "Which Benchmark?");
        options.addOption("d", true, "Duration (sec)?");
        options.addOption("i", true, "Benchmark Interval (μs)");
        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");
        options.addOption("s", true, "Bench/App Name ([moodle, wordpress, tpcc] in retro benchmark)");
        options.addOption("p1", true, "Percentage 1");
        options.addOption("p2", true, "Percentage 2");
        options.addOption("p3", true, "Percentage 3");
        options.addOption("p4", true, "Percentage 4");
        options.addOption("p5", true, "Percentage 5");
        options.addOption("notxn", false, "Disable XDST transaction.");
        options.addOption("skipLoad", false, "Skip data loading/table resets.");
        options.addOption("noProv", false, "Disable provenance tracing.");
        options.addOption("execId", true, "Start execution ID for retro-replay.");
        options.addOption("endExecId", true, "End execution ID for retro-replay.");
        options.addOption("retroMode", true, "Replay mode: 0-not replay, 1-single replay execID, 2-replay all [execId, endExecId), 3-selective replay [execId, endExecId).");
        options.addOption("bugFix", true, "The name of the bug fix functions, if provided, will use the new code. Moodle: [subscribe], WordPress: [comment, option]");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        int interval = 1000;
        if (cmd.hasOption("i")) {
            interval = Integer.parseInt(cmd.getOptionValue("i"));
        }
        int duration = 60;
        if (cmd.hasOption("d")) {
            duration = Integer.parseInt(cmd.getOptionValue("d"));
        }
        String mainHostAddr = "localhost";
        if (cmd.hasOption("mainHostAddr")) {
            mainHostAddr = cmd.getOptionValue("mainHostAddr");
        }

        if (cmd.hasOption("notxn")) {
            logger.info("Disabling XDST transction!");
            ApiaryConfig.XDBTransactions = false;
        } else {
            logger.info("Using XDST transaction!");
            ApiaryConfig.XDBTransactions = true;
        }

        if (cmd.hasOption("noProv")) {
            logger.info("Disabling provenance tracing!");
            ApiaryConfig.captureReads = false;
            ApiaryConfig.captureUpdates = false;
            ApiaryConfig.captureMetadata = false;
            ApiaryConfig.recordInput = false;
        } else {
            logger.info("Provenance tracing enabled!");
            ApiaryConfig.captureReads = true;
            ApiaryConfig.captureUpdates = true;
            ApiaryConfig.captureMetadata = true;
            ApiaryConfig.recordInput = true;
        }

        if (cmd.hasOption("skipLoad")) {
            skipLoadData = true;
        }

        String benchmark = cmd.getOptionValue("b");
        String roleBench = benchmark;
        if (cmd.hasOption("s")) {
            roleBench = cmd.getOptionValue("s");
            logger.info("Benchmark role name: {}", roleBench);
        }
        if (benchmark.equals("increment")) {
            logger.info("Increment Benchmark");
            IncrementBenchmark.benchmark(mainHostAddr, roleBench, interval, duration, false);
        } else if (benchmark.equals("stateless-increment")) {
            logger.info("Stateless Increment Benchmark");
            IncrementBenchmark.benchmark(mainHostAddr, roleBench, interval, duration, true);
        } else if (benchmark.equals("retwis")) {
            logger.info("Retwis Benchmark");
            RetwisBenchmark.benchmark(mainHostAddr, roleBench, interval, duration);
        } else if (benchmark.equals("esmicro")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 100;
            int percentageNew = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 0;
            int percentageUpdate = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            logger.info("Elasticsearch Microbenchmark {} {} {}", percentageRead, percentageNew, percentageUpdate);
            ElasticsearchMicrobenchmark.benchmark(mainHostAddr, interval, duration, percentageRead, percentageNew, percentageUpdate);
        } else if (benchmark.equals("shop")) {
            int percentageGetItem = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 90;
            int percentageCheckout = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 8;
            int percentageAppend = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 1;
            int percentageUpdate = cmd.hasOption("p4") ? Integer.parseInt(cmd.getOptionValue("p4")) : 1;
            logger.info("ShopBenchmark {} {} {} {}", percentageGetItem, percentageCheckout, percentageAppend, percentageUpdate);
            ShopBenchmark.benchmark(mainHostAddr, interval, duration, percentageGetItem, percentageCheckout, percentageAppend, percentageUpdate);
        } else if (benchmark.equals("hotel")) {
            int percentageSearch = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 80;
            int percentageReserve = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 20;
            logger.info("Hotel Benchmark {} {}", percentageSearch, percentageReserve);
            HotelBenchmark.benchmark(mainHostAddr, interval, duration, percentageSearch, percentageReserve);
        } else if (benchmark.equals("mongomicro")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 100;
            int percentageNew = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 0;
            int percentageUpdate = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            logger.info("Mongo Microbenchmark {} {} {}", percentageRead, percentageNew, percentageUpdate);
            MongoMicrobenchmark.benchmark(mainHostAddr, interval, duration, percentageRead, percentageNew, percentageUpdate);
        } else if (benchmark.equals("profile")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 90;
            int percentageNew = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 10;
            int percentageUpdate = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            logger.info("Profile Benchmark {} {} {}", percentageRead, percentageNew, percentageUpdate);
            ProfileBenchmark.benchmark(interval, duration, percentageRead, percentageNew, percentageUpdate);
        } else if (benchmark.equals("gcsmicro")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 100;
            int percentageNew = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 0;
            int percentageUpdate = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            logger.info("GCS Microbenchmark Benchmark {} {} {}", percentageRead, percentageNew, percentageUpdate);
            GCSMicrobenchmark.benchmark(interval, duration, percentageRead, percentageNew, percentageUpdate);
        } else if (benchmark.equals("superbenchmark")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 99;
            int percentageUpdate = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 1;
            logger.info("Superbenchmark {} {}", percentageRead, percentageUpdate);
            Superbenchmark.benchmark(mainHostAddr, interval, duration, percentageRead, percentageUpdate);
        } else if (benchmark.equals("mysqlmicro")) {
            int percentageRead = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 100;
            int percentageNew = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 0;
            int percentageUpdate = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            logger.info("Mysql Microbenchmark {} {} {}", percentageRead, percentageNew, percentageUpdate);
            MysqlMicrobenchmark.benchmark(mainHostAddr, interval, duration, percentageRead, percentageNew, percentageUpdate);
        } else if (benchmark.equals("retro")) {
            int p1 = cmd.hasOption("p1") ? Integer.parseInt(cmd.getOptionValue("p1")) : 0;
            int p2 = cmd.hasOption("p2") ? Integer.parseInt(cmd.getOptionValue("p2")) : 0;
            int p3 = cmd.hasOption("p3") ? Integer.parseInt(cmd.getOptionValue("p3")) : 0;
            int p4 = cmd.hasOption("p4") ? Integer.parseInt(cmd.getOptionValue("p4")) : 0;
            int p5 = cmd.hasOption("p5") ? Integer.parseInt(cmd.getOptionValue("p5")) : 0;
            logger.info("Retroactive Benchmark, App: {}, Percentages: {}, {}, {}, {}, {}", roleBench, p1, p2, p3, p4, p5);
            int retroMode = 0;
            long startExecId = 0l;
            long endExecId = cmd.hasOption("endExecId") ? Long.parseLong(cmd.getOptionValue("endExecId")) : Long.MAX_VALUE;
            String bugFix = cmd.hasOption("bugFix") ? cmd.getOptionValue("bugFix") : null;
            if (cmd.hasOption("retroMode")) {
                retroMode = Integer.parseInt(cmd.getOptionValue("retroMode"));
                if (retroMode > 0) {
                    startExecId = Long.parseLong(cmd.getOptionValue("execId"));
                }
                logger.info("Replay mode {}, startExecId: {}, endExecId: {}", retroMode, startExecId, endExecId);
            } else {
                logger.info("Not replay mode.");
            }
            RetroBenchmark.benchmark(roleBench, mainHostAddr, interval, duration, skipLoadData, retroMode, startExecId, endExecId, bugFix, List.of(p1, p2, p3, p4, p5));
        }
    }
}
