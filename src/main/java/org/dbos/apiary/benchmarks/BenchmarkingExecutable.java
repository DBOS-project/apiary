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
        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");
        options.addOption("s", true, "Service Name");
        options.addOption("p1", true, "Percentage 1");
        options.addOption("p2", true, "Percentage 2");
        options.addOption("p3", true, "Percentage 3");
        options.addOption("p4", true, "Percentage 4");

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
        String service = benchmark;
        if (cmd.hasOption("s")) {
            service = cmd.getOptionValue("s");
            logger.info("Service: {}", service);
        }
        if (benchmark.equals("increment")) {
            logger.info("Increment Benchmark");
            IncrementBenchmark.benchmark(mainHostAddr, service, interval, duration, false);
        } else if (benchmark.equals("stateless-increment")) {
            logger.info("Stateless Increment Benchmark");
            IncrementBenchmark.benchmark(mainHostAddr, service, interval, duration, true);
        } else if (benchmark.equals("retwis")) {
            logger.info("Retwis Benchmark");
            RetwisBenchmark.benchmark(mainHostAddr, service, interval, duration);
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
        }
    }
}
