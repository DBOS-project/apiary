package org.dbos.apiary;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.tests.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkerTests {
    private static final Logger logger = LoggerFactory.getLogger(WorkerTests.class);

    @Test
    public void testSerialization() {
        logger.info("testSerialization");
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        assertEquals(s.length, s2.length);
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
        int[] is = new int[]{1, 2, 3, 4, 3, 2, 1, 11234};
        int[] is2 = Utilities.byteArrayToIntArray(Utilities.intArrayToByteArray(is));
        assertEquals(is.length, is2.length);
        for (int i = 0; i < is2.length; i++) {
            assertEquals(is[i], is2[i]);
        }
    }

    // TODO: add more tests with Postgres as the backend.
}
