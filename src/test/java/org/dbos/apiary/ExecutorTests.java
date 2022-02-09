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
    public void testIncrement() throws IOException, ProcCallException {
        logger.info("testIncrement");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);
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
        logger.info("testAddition - direct invoke");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);
        VoltTable input = new VoltTable(
                new VoltTable.ColumnInfo("one", VoltType.BIGINT),
                new VoltTable.ColumnInfo("two", VoltType.BIGINT),
                new VoltTable.ColumnInfo("strings", VoltType.VARBINARY)
        );
        input.addRow(1, 2, Utilities.stringArraytoByteArray(new String[]{"matei", "zaharia"}));
        VoltTable[] res = ctxt.client.callProcedure("AdditionFunction", 0, input).getResults();
        assertEquals("3mateizaharia", res[0].fetchRow(0).getString(0));
        assertEquals(5, res[1].fetchRow(0).getColumnCount());
        assertEquals(4, res[2].fetchRow(0).getColumnCount());
        assertEquals(VoltType.SMALLINT, res[2].fetchRow(0).getColumnType(3));
        assertEquals(0, res[2].fetchRow(0).getLong(3));
    }

    @Test
    public void testAdditionExec() throws IOException, ProcCallException {
        logger.info("testAdditionExec - executor invoke");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);

        String res = Executor.executeFunction(ctxt, "AdditionFunction", 0, 1, 2, new String[]{"matei", "zaharia"});

        assertEquals("3mateizaharia", res);
    }

    @Test
    public void testFibExec() throws IOException, ProcCallException {
        logger.info("testFibExec - executor invoke, Fibonacci calculation.");
        ApiaryContext ctxt = new ApiaryContext("localhost", ApiaryConfig.voltdbPort);

        String res = Executor.executeFunction(ctxt, "FibonacciFunction", 1, "1");
        assertEquals("1", res);

        res = Executor.executeFunction(ctxt, "FibonacciFunction", 1, "3");
        assertEquals("2", res);
    }
}
