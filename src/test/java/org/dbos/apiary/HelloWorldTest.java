package org.dbos.apiary;

import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.utilities.Utilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ProcCallException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HelloWorldTest {
    private static final Logger logger = LoggerFactory.getLogger(HelloWorldTest.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testIncrement() throws IOException, ProcCallException {
        logger.info("testIncrement");
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        VoltTable[] res = ctxt.client.callProcedure("IncrementVSP", 0, 1L).getResults();
        assertEquals(1, res.length);
        long resVal = res[0].fetchRow(0).getLong(0);
        assertEquals(1L, resVal);
        // Run again, increment by one.
        res = ctxt.client.callProcedure("IncrementVSP", 0, 1L).getResults();
        resVal = res[0].fetchRow(0).getLong(0);
        assertEquals(2L, resVal);
    }

    @Test
    public void testAddition() throws IOException, ProcCallException {
        logger.info("testAddition");
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        VoltTable input = new VoltTable(new VoltTable.ColumnInfo("one", VoltType.INTEGER), new VoltTable.ColumnInfo("two", VoltType.INTEGER));
        input.addRow(1, 2);
        VoltTable[] res = ctxt.client.callProcedure("AdditionProcedure", 0, input).getResults();
        assertEquals("3", res[0].fetchRow(0).getString(0));
    }
}
