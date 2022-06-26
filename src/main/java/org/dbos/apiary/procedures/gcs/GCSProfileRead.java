package org.dbos.apiary.procedures.gcs;

import org.dbos.apiary.gcs.GCSContext;
import org.dbos.apiary.gcs.GCSFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class GCSProfileRead extends GCSFunction {

    public int runFunction(GCSContext context, int userID) throws SQLException {
        byte[] bytes = context.retrieve(ApiaryConfig.gcsTestBucket, Integer.toString(userID));
        if (bytes == null) {
            return -1;
        }
        return 0;
    }
}
