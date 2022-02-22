package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.Executor;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExecutorTests {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
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
        String res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "1");
        assertEquals("1", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "3");
        assertEquals("2", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "4");
        assertEquals("3", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "10");
        assertEquals("55", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "30");
        assertEquals("832040", res);
    }
}
