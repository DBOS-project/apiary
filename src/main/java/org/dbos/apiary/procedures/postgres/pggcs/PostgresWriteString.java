package org.dbos.apiary.procedures.postgres.pggcs;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresWriteString extends PostgresFunction {

    private static final String insert = "INSERT INTO StuffTable(Name, Stuff) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Stuff = EXCLUDED.Stuff;";

    public static String runFunction(PostgresContext ctxt, String name, String stuff) throws Exception {
        ctxt.apiaryCallFunction("GCSWriteString", name, stuff);
        ctxt.executeUpdate(insert, name, stuff);
        return name;
    }
}
