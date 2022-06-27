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
import org.dbos.apiary.procedures.gcs.GCSProfileRead;
import org.dbos.apiary.procedures.gcs.GCSProfileUpdate;
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
import org.dbos.apiary.procedures.postgres.pggcs.PostgresProfileRead;
import org.dbos.apiary.procedures.postgres.pggcs.PostgresProfileUpdate;
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
        ApiaryWorker apiaryWorker;

        if (db.equals("voltdb")) {
            throw new IllegalArgumentException("TODO: Implement VoltDB worker");
        } else if (db.equals("elasticsearch")) {
            apiaryWorker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            String esAddr = "localhost";
            if (cmd.hasOption("secondaryAddress")) {
                esAddr = cmd.getOptionValue("secondaryAddress");
                logger.info("Elasticsearch Address: {}", esAddr);
            }
            ElasticsearchConnection econn = new ElasticsearchConnection(esAddr, 9200, "elastic", "password");
            apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, econn);
            apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
            apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
            apiaryWorker.registerFunction("PostgresBulkIndexPerson", ApiaryConfig.postgres, PostgresBulkIndexPerson::new);
            apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
            apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
            apiaryWorker.registerFunction("ElasticsearchBulkIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchBulkIndexPerson::new);
            apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
            apiaryWorker.registerFunction("ShopAddItem", ApiaryConfig.postgres, ShopAddItem::new);
            apiaryWorker.registerFunction("ShopBulkAddItem", ApiaryConfig.postgres, ShopBulkAddItem::new);
            apiaryWorker.registerFunction("ShopSearchItem", ApiaryConfig.postgres, ShopSearchItem::new);
            apiaryWorker.registerFunction("ShopAddCart", ApiaryConfig.postgres, ShopAddCart::new);
            apiaryWorker.registerFunction("ShopCheckoutCart", ApiaryConfig.postgres, ShopCheckoutCart::new);
            apiaryWorker.registerFunction("ShopGetItem", ApiaryConfig.postgres, ShopGetItem::new);
            apiaryWorker.registerFunction("ShopESAddItem", ApiaryConfig.elasticsearch, ShopESAddItem::new);
            apiaryWorker.registerFunction("ShopESBulkAddItem", ApiaryConfig.elasticsearch, ShopESBulkAddItem::new);
            apiaryWorker.registerFunction("ShopESSearchItem", ApiaryConfig.elasticsearch, ShopESSearchItem::new);
        } else if (db.equals("mongo")) {
            apiaryWorker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            String mongoAddr = "localhost";
            if (cmd.hasOption("secondaryAddress")) {
                mongoAddr = cmd.getOptionValue("secondaryAddress");
                logger.info("Mongo Address: {}", mongoAddr);
            }
            MongoConnection mconn = new MongoConnection(mongoAddr, 27017);
            apiaryWorker.registerConnection(ApiaryConfig.mongo, mconn);
            apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
            apiaryWorker.registerFunction("PostgresAddHotel", ApiaryConfig.postgres, PostgresAddHotel::new);
            apiaryWorker.registerFunction("PostgresMakeReservation", ApiaryConfig.postgres, PostgresMakeReservation::new);
            apiaryWorker.registerFunction("PostgresSearchHotel", ApiaryConfig.postgres, PostgresSearchHotel::new);
            apiaryWorker.registerFunction("MongoMakeReservation", ApiaryConfig.mongo, MongoMakeReservation::new);
            apiaryWorker.registerFunction("MongoAddHotel", ApiaryConfig.mongo, MongoAddHotel::new);
            apiaryWorker.registerFunction("MongoSearchHotel", ApiaryConfig.mongo, MongoSearchHotel::new);
        } else if (db.equals("gcs")) {
            apiaryWorker = new ApiaryWorker(scheduler, numThreads);
            PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            GCSConnection gconn = new GCSConnection(pconn);
            apiaryWorker.registerConnection(ApiaryConfig.gcs, gconn);
            apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
            apiaryWorker.registerFunction("PostgresProfileUpdate", ApiaryConfig.postgres, PostgresProfileUpdate::new);
            apiaryWorker.registerFunction("PostgresProfileRead", ApiaryConfig.postgres, PostgresProfileRead::new);
            apiaryWorker.registerFunction("GCSProfileUpdate", ApiaryConfig.gcs, GCSProfileUpdate::new);
            apiaryWorker.registerFunction("GCSProfileRead", ApiaryConfig.gcs, GCSProfileRead::new);
        } else {
            throw new IllegalArgumentException("Option 'db' must be one of (elasticsearch, mongo, gcs).");
        }

        apiaryWorker.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Stopping Apiary worker server.");
            apiaryWorker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        apiaryWorker.shutdown();
    }
}
