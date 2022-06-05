package org.dbos.apiary.worker;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.increment.IncrementStatelessDriver;
import org.dbos.apiary.procedures.voltdb.retwis.RetwisMerge;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Executable for the worker daemon.
public class ApiaryWorkerExecutable {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerExecutable.class);

    // Ignore the illegal reflective access warning from VoltDB. TODO: Fix it later.
    public static void main(String[] args) throws Exception {
        logger.info("Starting Apiary worker server.");
        Options options = new Options();
        options.addOption("db", true,
                "The db used by this worker. Can be one of (voltdb, cockroachdb). Defaults to voltdb.");
        options.addOption("s", true, "Which Scheduler?");
        options.addOption("t", true, "How many worker threads?  Defaults to 128.");
        options.addOption("cockroachdbAddr", true, "Address of the CockroachDB server (e.g. localhost or ip addr).");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String db = "voltdb";
        if (cmd.hasOption("db")) {
            db = cmd.getOptionValue("db");
            logger.info("Using database: {}", db);
        }
        ApiaryConnection c;
        if (db.equals("voltdb")) {
            // Only need to connect to localhost.
            c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        } else {
            throw new IllegalArgumentException("Option 'db' must be one of (voltdb, cockroachdb).");
        }

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
        int numThreads = 128;
        if (cmd.hasOption("t")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("t"));
        }
        logger.info("{} worker threads", numThreads);
        ApiaryWorker worker;

        // Register all stateless functions for experiments.
        if (db.equals("voltdb")) {
            worker = new ApiaryWorker(scheduler, numThreads, ApiaryConfig.vertica, ApiaryConfig.provenanceDefaultAddress);
            worker.registerFunction("RetwisMerge", ApiaryConfig.stateless, RetwisMerge::new);
            worker.registerFunction("IncrementStatelessDriver", ApiaryConfig.stateless, IncrementStatelessDriver::new);
        } else {
            worker = new ApiaryWorker(scheduler, numThreads, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        }

        worker.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Stopping Apiary worker server.");
            worker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        worker.shutdown();
    }
}
