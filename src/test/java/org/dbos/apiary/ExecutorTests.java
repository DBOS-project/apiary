package org.dbos.apiary;

import com.google_voltpatches.common.base.Utf8;
import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.executor.Executor;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExecutorTests {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testSerialization() {
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
    }

    @Test
    public void testAdditionExec() throws IOException, ProcCallException {
        logger.info("testAdditionExec");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "AdditionFunction", 0, "1", "2", new String[]{"matei", "zaharia"});
        assertEquals("3mateizaharia", res);
    }

    @Test
    public void testFibExec() throws IOException, ProcCallException {
        logger.info("testFibExec");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "FibonacciFunction", 0, "1");
        assertEquals("1", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", 0, "3");
        assertEquals("2", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", 0, "4");
        assertEquals("3", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", 0, "10");
        assertEquals("55", res);
    }
}
