package org.dbos.apiary.worker;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchBulkIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchSearchPerson;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresBulkIndexPerson;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresIndexPerson;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresSearchPerson;
import org.dbos.apiary.utilities.ApiaryConfig;
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
                "The db used by this worker. Can be one of (voltdb, postgres). Defaults to postgres.");
        options.addOption("s", true, "Which Scheduler?");
        options.addOption("t", true, "How many worker threads?  Defaults to 128.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String db = "postgres";
        if (cmd.hasOption("db")) {
            db = cmd.getOptionValue("db");
            logger.info("Using database: {}", db);
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
        int numThreads = 64;
        if (cmd.hasOption("t")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("t"));
        }
        logger.info("{} worker threads", numThreads);
        ApiaryWorker worker;

        if (db.equals("voltdb")) {
            throw new IllegalArgumentException("TODO: Implement VoltDB worker");
        } else if (db.equals("postgres")) {
            worker = new ApiaryWorker(scheduler, numThreads, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            ElasticsearchConnection econn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            worker.registerConnection(ApiaryConfig.elasticsearch, econn);
            worker.registerConnection(ApiaryConfig.postgres, conn);
            worker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
            worker.registerFunction("PostgresBulkIndexPerson", ApiaryConfig.postgres, PostgresBulkIndexPerson::new);
            worker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
            worker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
            worker.registerFunction("ElasticsearchBulkIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchBulkIndexPerson::new);
            worker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
        } else {
            throw new IllegalArgumentException("Option 'db' must be one of (voltdb, postgres).");
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
