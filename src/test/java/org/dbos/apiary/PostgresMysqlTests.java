package org.dbos.apiary;


import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.MysqlQueryPerson;
import org.dbos.apiary.procedures.mysql.MysqlUpsertPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.PostgresQueryPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.PostgresUpsertPerson;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresMysqlTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresMysqlTests.class);

    private ApiaryWorker apiaryWorker;
    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }

        try {
            MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            conn.dropTable("PersonTable");
            // TODO: need to solve the primary key issue. Currently cannot have primary keys.
            conn.createTable("PersonTable", "Name varchar(1000) NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to MySQL.");
        }

        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testMysqlBasic() throws InvalidProtocolBufferException {
        logger.info("testMysqlBasic");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresUpsertPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMysqlUpdate() throws InvalidProtocolBufferException {
        logger.info("testMysqlUpdate");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresUpsertPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresUpsertPerson", "matei", 2).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

}
