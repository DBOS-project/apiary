package org.dbos.apiary.procedures.gcs;

import org.dbos.apiary.gcs.GCSContext;
import org.dbos.apiary.gcs.GCSFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class GCSWriteString extends GCSFunction {

    public String runFunction(GCSContext context, String name, String content) throws SQLException {
        context.create(ApiaryConfig.gcsTestBucket, name, content.getBytes(StandardCharsets.UTF_8), "text/plain");
        return name;
    }
}
