package org.dbos.apiary.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GCSContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(GCSContext.class);

    private static final String insert = "INSERT INTO VersionTable(Name, Version) VALUES (?, ?);";

    public final Storage storage;
    public final TransactionContext txc;
    private final Connection pg;

    final Map<String, List<String>> writtenKeys = new HashMap<>();

    public GCSContext(Storage storage, WorkerContext workerContext, TransactionContext txc,
                      String service, long execID, long functionID, Connection pg) {
        super(workerContext, service, execID, functionID);
        this.storage = storage;
        this.txc = txc;
        this.pg = pg;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String functionName, Object... inputs) throws Exception {
        return null;
    }

    public void create(String bucket, String name, byte[] bytes) throws SQLException {
        PreparedStatement ps = pg.prepareStatement(insert);
        ps.setString(1, name);
        ps.setLong(2, txc.txID);
        ps.executeUpdate();
        ps.close();
        BlobId blobID = BlobId.of(bucket, name);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobID).setContentType("text/plain").build();
        storage.create(blobInfo, bytes);
        writtenKeys.putIfAbsent(bucket, new ArrayList<>());
        writtenKeys.get(bucket).add(name);
    }

    public byte[] retrive(String bucket, String name) {
        BlobId blobID = BlobId.of(bucket, name);
        return storage.readAllBytes(blobID);
    }
}
