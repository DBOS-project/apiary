package org.dbos.apiary.procedures.gcs;

import org.dbos.apiary.gcs.GCSContext;
import org.dbos.apiary.gcs.GCSFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class GCSReadString extends GCSFunction {

    public String runFunction(GCSContext context, String name) throws SQLException {
        byte[] bytes = context.retrieve(ApiaryConfig.gcsTestBucket, name);
        if (bytes == null) {
            return "none";
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
