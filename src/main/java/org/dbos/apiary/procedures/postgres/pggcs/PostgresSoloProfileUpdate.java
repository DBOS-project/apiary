package org.dbos.apiary.procedures.postgres.pggcs;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSoloProfileUpdate extends PostgresFunction {

    public static String runFunction(PostgresContext ctxt, int userID, String pictureFile) throws Exception {
        ctxt.apiaryCallFunction("GCSProfileUpdate", userID, pictureFile);
        return pictureFile;
    }
}
