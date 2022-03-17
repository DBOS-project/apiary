package org.dbos.apiary.worker;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.retwis.RetwisMerge;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Executable for the worker daemon.
public class ApiaryWorkerExecutable {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerExecutable.class);

    // Ignore the illegal reflective access warning from VoltDB.  TODO: Fix it later.
    public static void main(String[] args) throws Exception {
        logger.info("Starting Apiary worker server.");
        Options options = new Options();
        options.addOption("s", true, "Which Scheduler?");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // Only need to connect to localhost.
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryScheduler scheduler = new ApiaryNaiveScheduler();
        if (cmd.hasOption("s")) {
            if (cmd.getOptionValue("s").equals("wfq")) {
                logger.info("Using WFQ Scheduler");
                scheduler = new ApiaryWFQScheduler();
            } else if (cmd.getOptionValue("s").equals("naive")) {
                logger.info("Using Naive Scheduler");
                scheduler = new ApiaryNaiveScheduler();
            }
        }

        ApiaryWorker worker = new ApiaryWorker(c, scheduler);

        // Register all stateless functions for experiments.
        worker.registerStatelessFunction("RetwisMerge", RetwisMerge::new);

        worker.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Stopping Apiary worker server.");
            worker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        worker.shutdown();
    }

}
