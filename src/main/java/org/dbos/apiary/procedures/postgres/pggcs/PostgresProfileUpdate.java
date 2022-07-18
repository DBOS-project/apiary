package org.dbos.apiary.procedures.postgres.pggcs;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresProfileUpdate extends PostgresFunction {

    private static final String insert = "INSERT INTO ProfileTable(UserID, Name, Status) VALUES (?, ?, ?) ON CONFLICT (UserID) DO UPDATE SET Status = EXCLUDED.Status;";

    public static String runFunction(PostgresContext ctxt, int userID, String name, String status, String pictureFile) throws Exception {
        ctxt.executeUpdate(insert, userID, name, status);
        ctxt.apiaryCallFunction("GCSProfileUpdate", userID, pictureFile);
        return name;
    }
}
