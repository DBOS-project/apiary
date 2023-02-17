package org.dbos.apiary;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
    public static final int provenancePort = 5432;  // Change to 5433 for a separate Postgres or Vertica.
    public static final String provenanceDB = ApiaryConfig.postgres; // Change to Vertica as needed.
    public static final String provenanceAddr = "localhost"; // Change to other addresses as needed.

    public static boolean testVoltConnection() {
        try {
            VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        } catch (Exception e) {
            logger.info("Failed to connect to VoltDB.");
            return false;
        }
        return true;
    }

    public static boolean testPostgresConnection() {
        // Set to the same port for testing.
        ApiaryConfig.provenancePort = provenancePort;
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", provenanceDB, provenanceAddr);
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
            return false;
        }
        return true;
    }

    public static boolean testESConnection() {
        try {
            ElasticsearchClient client = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
        } catch (Exception e) {
            logger.info("Failed to connect to ElasticSearch.");
            return false;
        }
        return true;
    }

    public static boolean testMongoConnection() {
        try {
            MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
            Bson command = new BsonDocument("ping", new BsonInt64(1));
            Document commandResult = conn.database.runCommand(command);
        } catch (Exception e) {
            logger.info("Failed to connect to Mongo! {}", e.getMessage());
            return false;
        }
        return true;
    }

    public static boolean testGCSConnection() {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Bucket bucket = storage.get(ApiaryConfig.gcsTestBucket);
        } catch (Exception e) {
            logger.info("No GCS instance! {}", e.getMessage());
            return false;
        }
        return true;
    }

    public static boolean testMysqlConnection() {
        try {
            MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
        } catch (Exception e) {
            logger.info("Failed to connect to MySQL.");
            return false;
        }
        return true;
    }
}
