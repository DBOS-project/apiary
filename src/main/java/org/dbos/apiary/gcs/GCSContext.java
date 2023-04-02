package org.dbos.apiary.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class GCSContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(GCSContext.class);

    private static final String insert = "INSERT INTO VersionTable(Name, BeginVersion, EndVersion) VALUES (?, ?, ?);";
    private static final String retrieve = "SELECT BeginVersion, EndVersion FROM VersionTable WHERE Name=? AND (BeginVersion<? OR BeginVersion=?);";
    private static final String update = "UPDATE VersionTable SET EndVersion=? WHERE Name=? AND BeginVersion<? AND EndVersion=?;";

    public final Storage storage;
    public final TransactionContext txc;
    private final Connection primary;

    final Map<String, List<String>> writtenKeys;
    private final Map<String, Map<String, AtomicBoolean>> lockManager;

    public GCSContext(Storage storage, Map<String, List<String>> writtenKeys, Map<String, Map<String, AtomicBoolean>> lockManager, WorkerContext workerContext,
                      TransactionContext txc, String role, long execID, long functionID, Connection primary) {
        super(workerContext, role, execID, functionID, ApiaryConfig.ReplayMode.NOT_REPLAY.getValue());
        this.storage = storage;
        this.txc = txc;
        this.primary = primary;
        this.lockManager = lockManager;
        this.writtenKeys = writtenKeys;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String functionName, Object... inputs) throws Exception {
        return null;
    }

    public void create(String bucket, String name, byte[] bytes, String contentType) throws SQLException {
        if (!ApiaryConfig.XDBTransactions) {
            BlobId blobID = BlobId.of(bucket, name);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobID).setContentType(contentType).build();
            storage.create(blobInfo, bytes);
            return;
        }
        lockManager.putIfAbsent(bucket, new ConcurrentHashMap<>());
        lockManager.get(bucket).putIfAbsent(name, new AtomicBoolean(false));
        boolean available = lockManager.get(bucket).get(name).compareAndSet(false, true);
        if (!available) {
            throw new PSQLException("tuple locked", PSQLState.SERIALIZATION_FAILURE);
        }
        writtenKeys.putIfAbsent(bucket, new ArrayList<>());
        writtenKeys.get(bucket).add(name);
        PreparedStatement ps = primary.prepareStatement(insert);
        ps.setString(1, name);
        ps.setLong(2, txc.txID);
        ps.setLong(3, Long.MAX_VALUE);
        ps.executeUpdate();
        ps.close();
        BlobId blobID = BlobId.of(bucket, name + txc.txID);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobID).setContentType(contentType).build();
        storage.create(blobInfo, bytes);
        ps = primary.prepareStatement(update);
        ps.setLong(1, txc.txID);
        ps.setString(2, name);
        ps.setLong(3, txc.txID);
        ps.setLong(4, Long.MAX_VALUE);
        ps.executeUpdate();
        ps.close();
    }

    public byte[] retrieve(String bucket, String name) throws SQLException {
        if (!ApiaryConfig.XDBTransactions) {
            BlobId blobID = BlobId.of(bucket, name);
            return storage.readAllBytes(blobID);
        }
        PreparedStatement ps = primary.prepareStatement(retrieve);
        ps.setString(1, name);
        ps.setLong(2, txc.xmax);
        ps.setLong(3, txc.txID);
        ResultSet rs = ps.executeQuery();
        long version = -1;
        while (rs.next()) {
            long beginVersion = rs.getLong(1);
            long endVersion = rs.getLong(2);
            if (!txc.activeTransactions.contains(beginVersion) && endVersion != txc.txID && (endVersion >= txc.xmax || txc.activeTransactions.contains(endVersion))) {
                version = beginVersion;
                break;
            }
        }
        if (version == -1) {
            return null;
        }
        rs.close();
        ps.close();
        BlobId blobID = BlobId.of(bucket, name + version);
        return storage.readAllBytes(blobID);
    }
}
