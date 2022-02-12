package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.Executor;
import org.dbos.apiary.procedures.FibonacciFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;

import static org.dbos.apiary.procedures.FibonacciFunction.FIBPKEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExecutorTests {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testSerialization() {
        logger.info("testSerialization");
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
    }

    @Test
    public void testAdditionExec() throws Exception {
        logger.info("testAdditionExec");
        ApiaryConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "AdditionFunction", 0, "1", "2", new String[]{"matei", "zaharia"});
        assertEquals("3mateizaharia", res);
    }

    @Test
    public void testFibExec() throws Exception {
        logger.info("testFibExec");
        ApiaryConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "FibonacciFunction", FibonacciFunction.FIBPKEY, "1");
        assertEquals("1", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", FibonacciFunction.FIBPKEY, "3");
        assertEquals("2", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", FibonacciFunction.FIBPKEY, "4");
        assertEquals("3", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", FibonacciFunction.FIBPKEY, "10");
        assertEquals("55", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", FIBPKEY, "30");
        assertEquals("832040", res);
    }
}
