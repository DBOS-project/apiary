package org.dbos.apiary.executor;

import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;

import java.io.IOException;

public class Executor {
    private static final Logger logger = LoggerFactory.getLogger(Executor.class);

    private static VoltTable objectInputToVoltTable(Object... rawInput) {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[rawInput.length];
        for (int i = 0; i < rawInput.length; i++) {
            Object input = rawInput[i];
            columns[i] = Utilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        for (int i = 0; i < rawInput.length; i++) {
            Object input = rawInput[i];
            if (input instanceof String[]) {
                row[i] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof Integer) {
                row[i] = input;
            } else if (input instanceof Double) {
                row[i] = input;
            } else if (input instanceof String) {
                row[i] = input;
            } else {
                logger.error("Do not support input type: {}, in parameter index {}", input.getClass().getName(), i);
                return null;
            }
        }
        v.addRow(row);
        return v;
    }

    // Execute the root function and return a single JSON string as the result.
    // TODO: better way to handle partition key, and support multi-partition functions (no pkey).
    public static String executeFunction(ApiaryContext ctxt, String funcName, long pkey, Object... rawInput)
            throws  IOException, ProcCallException {
        // Process input to VoltTable.
        VoltTable voltInput = objectInputToVoltTable(rawInput);

        // Invoke Stored procedure.
        VoltTable[] res = ctxt.client.callProcedure(funcName, pkey, voltInput).getResults();

        // The first VoltTable should be the result.
        String finalOutput = res[0].fetchRow(0).getString(0);
        return finalOutput;
    }
}
