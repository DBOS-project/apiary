package org.dbos.apiary.procedures.gcs;

import com.google.cloud.storage.BlobId;
import org.dbos.apiary.gcs.GCSContext;
import org.dbos.apiary.gcs.GCSFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.nio.charset.StandardCharsets;

public class GCSReadString extends GCSFunction {

    public String runFunction(GCSContext context, String name) {
        byte[] bytes = context.retrive(ApiaryConfig.gcsTestBucket, name);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
