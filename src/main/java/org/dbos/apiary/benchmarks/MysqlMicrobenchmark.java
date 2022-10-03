package org.dbos.apiary.benchmarks;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.MysqlBulkAddPerson;
import org.dbos.apiary.procedures.mysql.MysqlQueryPerson;
import org.dbos.apiary.procedures.mysql.MysqlReplacePerson;
import org.dbos.apiary.procedures.mysql.MysqlUpsertPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MysqlMicrobenchmark {
    private static final Logger logger = LoggerFactory.getLogger(MysqlMicrobenchmark.class);

    private static final int threadPoolSize = 128;
    private static final int numWorker = 16;

    private static final int numPeople = 1000000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> insertTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> updateTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageRead, int percentageAppend, int percentageUpdate) throws SQLException, InterruptedException, IOException {
        assert (percentageRead + percentageAppend + percentageUpdate == 100);
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureReads = false;

        PostgresConnection pgConn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        pgConn.dropTable("FuncInvocations");

        MysqlConnection mysqlConn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
        mysqlConn.dropTable("PersonTable");
        if (ApiaryConfig.XDBTransactions) {
            // TODO: need to solve the primary key issue. Currently cannot have primary keys.
            mysqlConn.createTable("PersonTable", "Name varchar(100) NOT NULL, Number integer NOT NULL");
        } else {
            mysqlConn.createTable("PersonTable", "Name varchar(100) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        }

        // Start serving.
        ApiaryWorker apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, mysqlConn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);
        apiaryWorker.registerFunction("PostgresMysqlSoloAddPerson", ApiaryConfig.postgres, PostgresMysqlSoloAddPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloBulkAddPerson", ApiaryConfig.postgres, PostgresMysqlSoloBulkAddPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloQueryPerson", ApiaryConfig.postgres, PostgresMysqlSoloQueryPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloReplacePerson", ApiaryConfig.postgres, PostgresMysqlSoloReplacePerson::new);

        apiaryWorker.registerFunction("MysqlBulkAddPerson", ApiaryConfig.mysql, MysqlBulkAddPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);
        apiaryWorker.registerFunction("MysqlReplacePerson", ApiaryConfig.mysql, MysqlReplacePerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);


        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        long loadStart = System.currentTimeMillis();
        String[] names = new String[numPeople];
        int[] nums = new int[numPeople];
        for (int personNum = 0; personNum < numPeople; personNum++) {
            names[personNum] = "matei" + personNum;
            nums[personNum] = personNum;
        }
        client.get().executeFunction("PostgresMysqlSoloBulkAddPerson", names, nums);
        int res = client.get().executeFunction("PostgresMysqlSoloQueryPerson", "matei" + 0).getInt();
        assert (res == 0);
        logger.info("Done loading {} people: {}ms", numPeople, System.currentTimeMillis() - loadStart);

    }
}
