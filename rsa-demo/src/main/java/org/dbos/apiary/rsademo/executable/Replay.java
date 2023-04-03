package org.dbos.apiary.rsademo.executable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.RetroBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.MDLUtil;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class Replay {

    private static final Logger logger = LoggerFactory.getLogger(Replay.class);

    public static void replay(long startExecId, long endExecId) throws InvalidProtocolBufferException {

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        long startTime = System.currentTimeMillis();
        FunctionOutput res = client.retroReplay(startExecId, endExecId, ApiaryConfig.ReplayMode.ALL.getValue());
        assert (res != null);
        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("Replay execution time: {} ms", elapsedTime);
    }
}
