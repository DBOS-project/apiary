package org.dbos.apiary.gcs;

import com.google.cloud.storage.*;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class GCSConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(GCSConnection.class);

    public final Storage storage;
    private final PostgresConnection primary;

    private static final String findDeletable = "SELECT Name, BeginVersion FROM VersionTable WHERE EndVersion<?";
    private static final String delete = "DELETE FROM VersionTable WHERE Name=? AND BeginVersion=?";
    private static final String undoUpdate = "UPDATE VersionTable SET EndVersion=? WHERE EndVersion=?;";

    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();
    private final Map<String, Map<String, AtomicBoolean>> lockManager = new ConcurrentHashMap<>();

    public GCSConnection(PostgresConnection primary) {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.primary = primary;
        storage.get(ApiaryConfig.gcsTestBucket);
    }

    @Override
    public FunctionOutput callFunction(String functionName, Map<String, List<String>> writtenKeys, WorkerContext workerContext,
                                       TransactionContext txc, String role,
                                       long execID, long functionID,
                                       Object... inputs) throws Exception {
        GCSContext ctxt = new GCSContext(storage, writtenKeys, lockManager, workerContext, txc, role, execID, functionID, primary.connection.get());
        return workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
}

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String bucket: writtenKeys.keySet()) {
            List<BlobId> blobIDs = writtenKeys.get(bucket).stream().map(i -> BlobId.of(bucket, i + txc.txID)).collect(Collectors.toList());
            List<Blob> blobs = storage.get(blobIDs);
            blobs.forEach(Blob::delete);
            for (String key: writtenKeys.get(bucket)) {
                lockManager.get(bucket).get(key).set(false);
            }
        }
    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        Set<Long> activeTransactions = new HashSet<>(txc.activeTransactions);
        validationLock.lock();
        boolean valid = true;
        for (String bucket: writtenKeys.keySet()) {
            for (String key : writtenKeys.get(bucket)) {
                // Has the key been modified by a transaction not in the snapshot?
                Set<Long> writes = committedWrites.getOrDefault(bucket, Collections.emptyMap()).getOrDefault(key, Collections.emptySet());
                for (Long write : writes) {
                    if (write >= txc.xmax || activeTransactions.contains(write)) {
                        valid = false;
                        break;
                    }
                }
            }
        }
        if (valid) {
            for (String bucket: writtenKeys.keySet()) {
                for (String key : writtenKeys.get(bucket)) {
                    committedWrites.putIfAbsent(bucket, new ConcurrentHashMap<>());
                    committedWrites.get(bucket).putIfAbsent(key, ConcurrentHashMap.newKeySet());
                    committedWrites.get(bucket).get(key).add(txc.txID);
                }
            }
        }
        validationLock.unlock();
        return valid;

    }

    @Override
    public void commit(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String bucket: writtenKeys.keySet()) {
            for (String key : writtenKeys.get(bucket)) {
                lockManager.get(bucket).get(key).set(false);
            }
        }
    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        // No need to keep track of writes that are visible to all active or future transactions.
        committedWrites.values().forEach(i -> i.values().forEach(w -> w.removeIf(txID -> txID < globalxmin)));
        // Delete old versions that are no longer visible to any active or future transaction.
        try {
            Connection c = primary.connection.get();
            PreparedStatement psFind = c.prepareStatement(findDeletable);
            PreparedStatement psDelete = c.prepareStatement(delete);
            List<String> deleteNames = new ArrayList<>();
            List<Long> deleteVersions = new ArrayList<>();
            psFind.setLong(1, globalxmin);
            ResultSet rs = psFind.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                long beginVersion = rs.getLong(2);
                deleteNames.add(name);
                deleteVersions.add(beginVersion);
                psDelete.setString(1, name);
                psDelete.setLong(2, beginVersion);
                psDelete.executeUpdate();

            }
            c.commit();
            Bucket bucket = storage.get(ApiaryConfig.gcsTestBucket);  // TODO: More buckets.
            for (int i = 0; i < deleteNames.size(); i++) {
                String name = deleteNames.get(i);
                long beginVersion = deleteVersions.get(i);
                bucket.get(name + beginVersion).delete();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
