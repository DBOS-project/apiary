package org.dbos.apiary.benchmarks;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingExecutable {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkingExecutable.class);

    // Ignore the illegal reflective access warning from VoltDB. TODO: Fix it later.
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("b", true, "Which Benchmark?");
        options.addOption("d", true, "Duration (sec)?");
        options.addOption("i", true, "Benchmark Interval (μs)");
        options.addOption("i2", true, "Mixed Benchmark Increment Interval (μs)");
        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");

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
        String benchmark = cmd.getOptionValue("b");
        if (benchmark.equals("increment")) {
            logger.info("Increment Benchmark");
            IncrementBenchmark.benchmark(mainHostAddr, interval, duration);
        } else if (benchmark.equals("retwis")) {
            logger.info("Retwis Benchmark");
            RetwisBenchmark.benchmark(mainHostAddr, interval, duration);
        } else if (benchmark.equals("mixed")) {
            logger.info("Mixed Benchmark");
            int interval2 = Integer.parseInt(cmd.getOptionValue("i2"));
            MixedBenchmark.benchmark(mainHostAddr, interval, interval2, duration);
        } else if (benchmark.equals("cockroach-increment")) {
            logger.info("CockroachDB Increment Benchmark");
            CockroachDBIncrementBenchmark.benchmark(mainHostAddr, interval, duration);
        }
    }
}
