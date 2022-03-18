package org.dbos.apiary.worker;

import java.sql.SQLException;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.increment.IncrementStatelessDriver;
import org.dbos.apiary.procedures.voltdb.retwis.RetwisMerge;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dbos.apiary.procedures.cockroachdb.CockroachDBIncrementFunction;
import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibSumFunction;
import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibonacciFunction;
import org.postgresql.ds.PGSimpleDataSource;
import org.dbos.apiary.cockroachdb.CockroachDBConnection;

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
            c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        } else if (db.equals("cockroachdb")) {
            c = getCockroachDBConnection(
                    /* cockroachdbAddr= */cmd.hasOption("cockroachdbAddr") ? cmd.getOptionValue("cockroachdbAddr")
                            : "localhost");
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
        ApiaryWorker worker = new ApiaryWorker(c, scheduler);

        // Register all stateless functions for experiments.
        if (db.equals("voltdb")) {
            worker.registerStatelessFunction("RetwisMerge", RetwisMerge::new);
            worker.registerStatelessFunction("IncrementStatelessDriver", IncrementStatelessDriver::new);
        }

        worker.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Stopping Apiary worker server.");
            worker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        worker.shutdown();
    }

    private static ApiaryConnection getCockroachDBConnection(String cockroachdbAddr) throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] { cockroachdbAddr });
        ds.setPortNumbers(new int[] { ApiaryConfig.cockroachdbPort });
        ds.setDatabaseName("test");
        ds.setUser("root");
        ds.setSsl(false);

        CockroachDBConnection c = new CockroachDBConnection(ds, /* tableName= */"KVTable");

        c.registerFunction("IncrementFunction", () -> {
            return new CockroachDBIncrementFunction(c.getConnectionForFunction());
        });
        c.registerFunction("FibonacciFunction", () -> {
            return new CockroachDBFibonacciFunction(c.getConnectionForFunction());
        });
        c.registerFunction("FibSumFunction", () -> {
            return new CockroachDBFibSumFunction(c.getConnectionForFunction());
        });

        return c;
    }
}
