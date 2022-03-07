package org.dbos.apiary.worker;

import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibSumFunction;
import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibonacciFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.ds.PGSimpleDataSource;
import org.dbos.apiary.cockroachdb.CockroachDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;

// Executable for the worker daemon.
public class CockroachDBApiaryWorkerExecutable {
    private static final Logger logger = LoggerFactory.getLogger(CockroachDBApiaryWorkerExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("cockroachdbAddr", true, "Address of the CockroachDB server (e.g. localhost or ip addr).");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String cockroachdbAddr = "localhost";
        if (cmd.hasOption("cockroachdbAddr")) {
            cockroachdbAddr = cmd.getOptionValue("cockroachdbAddr");
        }

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] { cockroachdbAddr });
        ds.setPortNumbers(new int[] { 26257 });
        ds.setDatabaseName("test");
        ds.setUser("root");
        ds.setSsl(false);

        Connection conn = ds.getConnection();

        logger.info("Starting Apiary worker server.");
        CockroachDBConnection c = new CockroachDBConnection(conn, /* tableName= */"KVTable");
        
        c.registerFunction("FibonacciFunction", () -> new CockroachDBFibonacciFunction(conn));
        c.registerFunction("FibSumFunction", () -> new CockroachDBFibSumFunction(conn));
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.startServing();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Stopping Apiary worker server.");
            worker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        worker.shutdown();
    }

}
