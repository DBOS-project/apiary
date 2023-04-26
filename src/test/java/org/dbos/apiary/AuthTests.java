package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.tests.PostgresIncrementFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AuthTests {
    private static final Logger logger = LoggerFactory.getLogger(AuthTests.class);

    private ApiaryWorker apiaryWorker;

    // Local provenance config.
    private static final int provenancePort = ApiaryConfig.postgresPort;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;
        ApiaryConfig.provenancePort = provenancePort;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable(ApiaryConfig.tableFuncInvocations);
            conn.dropTable(ApiaryConfig.tableRecordedInputs);
            conn.dropTable("KVTable");
            conn.createTable("KVTable", "KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL");
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            assumeTrue(false);
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
    public void testRolesBasic() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testRolesBasic");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("Increment", ApiaryConfig.postgres, PostgresIncrementFunction::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient defClient = new ApiaryWorkerClient("localhost");  // Use the default role.
        int res;
        res = defClient.executeFunction("Increment", 100).getInt();
        assertEquals(1, res);

        // Then create another client with a specified role.
        ApiaryWorkerClient specialClient = new ApiaryWorkerClient("localhost", "specialUser");
        res = specialClient.executeFunction("Increment", 100).getInt();
        assertEquals(2, res);

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();

        // Check function invocation table.
        String table = ApiaryConfig.tableFuncInvocations;
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s ASC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        String resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        Assertions.assertEquals(ApiaryConfig.defaultRole, resRole);
        Assertions.assertEquals("Increment", resFuncName);

        rs.next();
        resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
        resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        Assertions.assertEquals("specialUser", resRole);
        Assertions.assertEquals("Increment", resFuncName);
    }

    @Test
    public void testFunctionRoles() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testFunctionRoles");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        String specialUser = "specialUser";
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("Increment", ApiaryConfig.postgres, PostgresIncrementFunction::new);
        apiaryWorker.restrictFunction("Increment", Set.of(specialUser));
        apiaryWorker.startServing();

        ApiaryWorkerClient defClient = new ApiaryWorkerClient("localhost");  // Use the default role.
        int res;
        res = defClient.executeFunction("Increment", 100).getInt();
        assertEquals(-1, res);

        // Then create another client with a specified role.
        ApiaryWorkerClient specialClient = new ApiaryWorkerClient("localhost", specialUser);
        res = specialClient.executeFunction("Increment", 100).getInt();
        assertEquals(1, res);
    }

    @Test
    public void testSuspendRole() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testSuspendRole");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        String specialUser = "specialUser";
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("Increment", ApiaryConfig.postgres, PostgresIncrementFunction::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient specialClient = new ApiaryWorkerClient("localhost", specialUser);
        int res;
        res = specialClient.executeFunction("Increment", 100).getInt();
        assertEquals(1, res);

        apiaryWorker.suspendRole(specialUser);

        res = specialClient.executeFunction("Increment", 100).getInt();
        assertEquals(-1, res);

        apiaryWorker.restoreRole(specialUser);

        res = specialClient.executeFunction("Increment", 100).getInt();
        assertEquals(2, res);
    }
}
