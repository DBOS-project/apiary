package org.dbos.apiary.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.postgres.PostgresConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GCSContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(GCSContext.class);

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

    public void create(BlobInfo blobInfo, byte[] bytes) throws SQLException {
        Statement s = pg.createStatement();
        s.execute(String.format("INSERT INTO VersionTable(Name, Version) VALUES (%s, %d);", blobInfo.getName(), txc.txID));
        s.close();
        storage.create(blobInfo, bytes);
        writtenKeys.putIfAbsent(blobInfo.getBucket(), new ArrayList<>());
        writtenKeys.get(blobInfo.getBucket()).add(blobInfo.getBlobId().getName());
    }

    public byte[] retrive(BlobId blobID) {
        return storage.readAllBytes(blobID);
    }
}
