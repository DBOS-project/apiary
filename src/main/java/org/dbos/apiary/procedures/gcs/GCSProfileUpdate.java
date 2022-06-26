package org.dbos.apiary.procedures.gcs;

import org.dbos.apiary.gcs.GCSContext;
import org.dbos.apiary.gcs.GCSFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

public class GCSProfileUpdate extends GCSFunction {

    public int runFunction(GCSContext context, int userID, String pictureFile) throws SQLException, IOException {
        byte[] pictureBytes = Files.readAllBytes(Path.of(pictureFile));
        context.create(ApiaryConfig.gcsTestBucket, Integer.toString(userID), pictureBytes, "image/jpeg");
        return 0;
    }
}
