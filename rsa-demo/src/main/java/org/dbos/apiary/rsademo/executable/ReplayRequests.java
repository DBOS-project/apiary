package org.dbos.apiary.rsademo.executable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.rsademo.functions.NectarAddPost;
import org.dbos.apiary.rsademo.functions.NectarGetPosts;
import org.dbos.apiary.rsademo.functions.NectarLogin;
import org.dbos.apiary.rsademo.functions.NectarRegister;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class ReplayRequests {

    private static final Logger logger = LoggerFactory.getLogger(ReplayRequests.class);

    private static int provenancePort = 5432;  // Change to 5433 for a separate Postgres or Vertica.
    private static String provenanceDB = ApiaryConfig.postgres; // Change to Vertica as needed.
    private static String provenanceAddr = "localhost"; // Change to other addresses as needed.

    private static final int numWorker = 4;

    public static void replay(long startExecId, long endExecId) throws SQLException, InvalidProtocolBufferException {

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        long startTime = System.currentTimeMillis();
        FunctionOutput res = client.retroReplay(startExecId, endExecId, ApiaryConfig.ReplayMode.ALL.getValue());
        assert (res != null);
        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("Replay execution time: {} ms", elapsedTime);
    }
}
