package org.dbos.apiary.worker;

import org.apache.commons_voltpatches.cli.CommandLine;
import org.apache.commons_voltpatches.cli.CommandLineParser;
import org.apache.commons_voltpatches.cli.DefaultParser;
import org.apache.commons_voltpatches.cli.Options;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.gcs.GCSConnection;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchBulkIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchSearchPerson;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopESAddItem;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopESBulkAddItem;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopESSearchItem;
import org.dbos.apiary.procedures.gcs.GCSReadString;
import org.dbos.apiary.procedures.gcs.GCSWriteString;
import org.dbos.apiary.procedures.mongo.hotel.MongoAddHotel;
import org.dbos.apiary.procedures.mongo.hotel.MongoMakeReservation;
import org.dbos.apiary.procedures.mongo.hotel.MongoSearchHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresAddHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresMakeReservation;
import org.dbos.apiary.procedures.postgres.hotel.PostgresSearchHotel;
import org.dbos.apiary.procedures.postgres.pges.PostgresBulkIndexPerson;
import org.dbos.apiary.procedures.postgres.pges.PostgresIndexPerson;
import org.dbos.apiary.procedures.postgres.pges.PostgresSearchPerson;
import org.dbos.apiary.procedures.postgres.pggcs.PostgresReadString;
import org.dbos.apiary.procedures.postgres.pggcs.PostgresWriteString;
import org.dbos.apiary.procedures.postgres.shop.*;
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
                "The secondary used by this worker.");
        options.addOption("s", true, "Which Scheduler?");
        options.addOption("t", true, "How many worker threads?");
        options.addOption("secondaryAddress", true, "Secondary Address.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String db;
        if (cmd.hasOption("db")) {
            db = cmd.getOptionValue("db");
            logger.info("Using database: {}", db);
        } else {
            logger.info("No database!");
            return;
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
        } else if (db.equals("elasticsearch")) {
            worker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            String esAddr = "localhost";
            if (cmd.hasOption("secondaryAddress")) {
                esAddr = cmd.getOptionValue("secondaryAddress");
                logger.info("Elasticsearch Address: {}", esAddr);
            }
            ElasticsearchConnection econn = new ElasticsearchConnection(esAddr, 9200, "elastic", "password");
            worker.registerConnection(ApiaryConfig.elasticsearch, econn);
            worker.registerConnection(ApiaryConfig.postgres, conn);
            worker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
            worker.registerFunction("PostgresBulkIndexPerson", ApiaryConfig.postgres, PostgresBulkIndexPerson::new);
            worker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
            worker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
            worker.registerFunction("ElasticsearchBulkIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchBulkIndexPerson::new);
            worker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
            worker.registerFunction("ShopAddItem", ApiaryConfig.postgres, ShopAddItem::new);
            worker.registerFunction("ShopBulkAddItem", ApiaryConfig.postgres, ShopBulkAddItem::new);
            worker.registerFunction("ShopSearchItem", ApiaryConfig.postgres, ShopSearchItem::new);
            worker.registerFunction("ShopAddCart", ApiaryConfig.postgres, ShopAddCart::new);
            worker.registerFunction("ShopCheckoutCart", ApiaryConfig.postgres, ShopCheckoutCart::new);
            worker.registerFunction("ShopGetItem", ApiaryConfig.postgres, ShopGetItem::new);
            worker.registerFunction("ShopESAddItem", ApiaryConfig.elasticsearch, ShopESAddItem::new);
            worker.registerFunction("ShopESBulkAddItem", ApiaryConfig.elasticsearch, ShopESBulkAddItem::new);
            worker.registerFunction("ShopESSearchItem", ApiaryConfig.elasticsearch, ShopESSearchItem::new);
        } else if (db.equals("mongo")) {
            worker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            String mongoAddr = "localhost";
            if (cmd.hasOption("secondaryAddress")) {
                mongoAddr = cmd.getOptionValue("secondaryAddress");
                logger.info("Mongo Address: {}", mongoAddr);
            }
            MongoConnection mconn = new MongoConnection(mongoAddr, 27017);
            worker.registerConnection(ApiaryConfig.mongo, mconn);
            worker.registerConnection(ApiaryConfig.postgres, conn);
            worker.registerFunction("PostgresAddHotel", ApiaryConfig.postgres, PostgresAddHotel::new);
            worker.registerFunction("PostgresMakeReservation", ApiaryConfig.postgres, PostgresMakeReservation::new);
            worker.registerFunction("PostgresSearchHotel", ApiaryConfig.postgres, PostgresSearchHotel::new);
            worker.registerFunction("MongoMakeReservation", ApiaryConfig.mongo, MongoMakeReservation::new);
            worker.registerFunction("MongoAddHotel", ApiaryConfig.mongo, MongoAddHotel::new);
            worker.registerFunction("MongoSearchHotel", ApiaryConfig.mongo, MongoSearchHotel::new);
        } else if (db.equals("gcs")) {
            worker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            GCSConnection gconn = new GCSConnection(pconn);
            worker.registerConnection(ApiaryConfig.gcs, gconn);
            worker.registerConnection(ApiaryConfig.postgres, pconn);
            worker.registerFunction("PostgresWriteString", ApiaryConfig.postgres, PostgresWriteString::new);
            worker.registerFunction("PostgresReadString", ApiaryConfig.postgres, PostgresReadString::new);
            worker.registerFunction("GCSWriteString", ApiaryConfig.gcs, GCSWriteString::new);
            worker.registerFunction("GCSReadString", ApiaryConfig.gcs, GCSReadString::new);
        } else {
            throw new IllegalArgumentException("Option 'db' must be one of (elasticsearch, mongo, gcs).");
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
