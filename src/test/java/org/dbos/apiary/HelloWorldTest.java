package org.dbos.apiary;

import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.utilities.Utilities;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HelloWorldTest {
    private static final Logger logger = LoggerFactory.getLogger(HelloWorldTest.class);

    @Test
    public void testHelloWorld() {
        logger.info("Hello, world!");
        String helloWorld = "Hello, world!";
        helloWorld = (String) Utilities.byteArrayToObject(Utilities.objectToByteArray(helloWorld));
        logger.info("{}", helloWorld);
    }

    @Test
    public void testIncrement() throws IOException, ProcCallException {
        logger.info("Hello, world Increment!");
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        VoltTable[] res = ctxt.client.callProcedure("IncrementVSP", 0, 1).getResults();
        assertEquals(1, res.length);
        long resVal = res[0].fetchRow(0).getLong(0);
        assertEquals(1, resVal);
    }
}
