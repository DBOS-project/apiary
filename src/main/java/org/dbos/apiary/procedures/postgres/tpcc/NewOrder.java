package org.dbos.apiary.procedures.postgres.tpcc;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class NewOrder extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt) {

        return 0;
    }
    @Override
    public boolean isReadOnly() { return false; }
}
