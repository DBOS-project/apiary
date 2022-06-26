package org.dbos.apiary.gcs;

import com.google.cloud.storage.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;

public class GCSConnection implements ApiarySecondaryConnection {

    public final Storage storage;
    private final PostgresConnection primary;

    private static final String update = "UPDATE VersionTable SET EndVersion=? WHERE Name=? AND BeginVersion<? AND EndVersion=?;";
    private static final String findDeletable = "SELECT Name, BeginVersion FROM VersionTable WHERE EndVersion<?";
    private static final String delete = "DELETE FROM VersionTable WHERE Name=? AND BeginVersion=?";

    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();

    public GCSConnection(PostgresConnection primary) {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.primary = primary;
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext,
                                       TransactionContext txc, String service,
                                       long execID, long functionID,
                                       Object... inputs) throws Exception {
        GCSContext ctxt = new GCSContext(storage, workerContext, txc, service, execID, functionID, primary.connection.get());
        FunctionOutput f = null;
        try {
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return f;
}

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String bucket: writtenKeys.keySet()) {
            List<BlobId> blobIDs = writtenKeys.get(bucket).stream().map(i -> BlobId.of(bucket, i + txc.txID)).collect(Collectors.toList());
            List<Blob> blobs = storage.get(blobIDs);
            blobs.forEach(Blob::delete);
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
        if (valid) {
            try {
                for (String bucket : writtenKeys.keySet()) {
                    Connection c = primary.connection.get();
                    PreparedStatement ps = c.prepareStatement(update);
                    for (String key : writtenKeys.get(bucket)) {
                        ps.setLong(1, txc.txID);
                        ps.setString(2, key);
                        ps.setLong(3, txc.txID);
                        ps.setLong(4, Long.MAX_VALUE);
                        ps.executeUpdate();
                    }
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
        return valid;

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
            for (String bucketName: committedWrites.keySet()) {
                Bucket bucket = storage.get(bucketName);
                psFind.setLong(1, globalxmin);
                ResultSet rs = psFind.executeQuery();
                while (rs.next()) {
                    String name = rs.getString(1);
                    long beginVersion = rs.getLong(2);
                    psDelete.setString(1, name);
                    psDelete.setLong(2, beginVersion);
                    psDelete.executeUpdate();
                    bucket.get(name + beginVersion).delete();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
