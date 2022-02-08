package org.dbos.apiary;

import com.google_voltpatches.common.base.Utf8;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExecutorTests {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testIncrement() throws IOException, ProcCallException {
        logger.info("testIncrement");
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        VoltTable[] res = ctxt.client.callProcedure("IncrementProcedure", 0, 1L).getResults();
        assertEquals(1, res.length);
        long resVal = res[0].fetchRow(0).getLong(0);
        assertEquals(1L, resVal);
        // Run again, increment by one.
        res = ctxt.client.callProcedure("IncrementProcedure", 0, 1L).getResults();
        resVal = res[0].fetchRow(0).getLong(0);
        assertEquals(2L, resVal);
    }

    @Test
    public void testSerialization() {
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            long t0 = System.nanoTime();
            String[] s3 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
            long t3 = System.nanoTime() - t0;
            times.add(t3);
        }
        logger.info("{}", times.stream().mapToLong(i -> i).average());
    }

    @Test
    public void testAddition() throws IOException, ProcCallException {
        logger.info("testAddition");
        ApiaryContext ctxt = new ApiaryContext("localhost", 21212);
        VoltTable input = new VoltTable(
                new VoltTable.ColumnInfo("one", VoltType.INTEGER),
                new VoltTable.ColumnInfo("two", VoltType.INTEGER),
                new VoltTable.ColumnInfo("strings", VoltType.VARBINARY)
        );
        input.addRow(1, 2, Utilities.stringArraytoByteArray(new String[]{"matei", "zaharia"}));
        VoltTable[] res = ctxt.client.callProcedure("AdditionFunction", 0, input).getResults();
        assertEquals("3mateizaharia", res[0].fetchRow(0).getString(0));
    }
}
