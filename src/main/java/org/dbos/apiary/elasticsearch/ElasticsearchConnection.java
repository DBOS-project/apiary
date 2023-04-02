package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.StoredScriptId;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ElasticsearchConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class);
    public ElasticsearchClient client;

    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();
    public static final String updateEndVersionScript = "updateEndVersion";

    Map<String, Map<String, AtomicBoolean>> lockManager = new ConcurrentHashMap<>();

    public ElasticsearchConnection(String hostname, int port, String username, String password) {
        try {
            Path caCertificatePath = Paths.get(System.getenv("ES_HOME") + "/config/certs/http_ca.crt");
            CertificateFactory factory =
                    CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = Files.newInputStream(caCertificatePath)) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            final SSLContext sslContext = sslContextBuilder.build();

            // Create the low-level client
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            RestClient restClient = RestClient.builder(
                            new HttpHost(hostname, port, "https"))
                    .setHttpClientConfigCallback(h -> h.setSSLContext(sslContext).setDefaultCredentialsProvider(credentialsProvider)).build();

            // Create the transport with a Jackson mapper
            ElasticsearchTransport transport = new RestClientTransport(
                    restClient, new JacksonJsonpMapper());

            // And create the API client
            this.client = new ElasticsearchClient(transport);

            // Store scripts
            client.putScript(p -> p.id(updateEndVersionScript).script(s -> s.lang("painless").source("ctx._source.endVersion=params['endV']")));
        } catch (CertificateException | IOException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            logger.info("Elasticsearch Connection Failed");
            throw new RuntimeException("Failed to connect to ElasticSearch");
        }
    }

    @Override
    public FunctionOutput callFunction(String functionName, Map<String, List<String>> writtenKeys, WorkerContext workerContext, TransactionContext txc, String role, long execID, long functionID, Object... inputs) throws Exception {
        ApiaryContext ctxt = new ElasticsearchContext(client, writtenKeys, lockManager, workerContext, txc, role, execID, functionID);
        return workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
    }

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        // Delete all records written by this transaction.
        for (String index : writtenKeys.keySet()) {
            try {
                client.indices().refresh(r -> r.index(index));
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    client.deleteByQuery(ubq -> ubq
                            .index(index)
                            .query(MatchQuery.of(t -> t.field("beginVersion").query(txc.txID))._toQuery())
                    );
                    client.updateByQuery(ubq -> ubq
                            .index(index)
                            .query(
                                    TermQuery.of(t -> t.field("endVersion").value(txc.txID))._toQuery()
                            ).script(s -> s
                                    .stored(StoredScriptId.of(ss -> ss
                                            .id(ElasticsearchConnection.updateEndVersionScript).params(Map.of("endV", JsonData.of(Long.MAX_VALUE))))
                                    )
                            )
                    );
                    client.indices().refresh(r -> r.index(index));
                    break;
                } catch (IOException e) {
                    // Retry on version conflict.
                    continue;
                }
            }
            for (String key: writtenKeys.get(index)) {
                lockManager.get(index).get(key).set(false);
            }
        }
    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        if (!ApiaryConfig.XDBTransactions) {
            return true;
        }
        Set<Long> activeTransactions = new HashSet<>(txc.activeTransactions);
        validationLock.lock();
        boolean valid = true;
        for (String index: writtenKeys.keySet()) {
            for (String key : writtenKeys.get(index)) {
                // Has the key been modified by a transaction not in the snapshot?
                Set<Long> writes = committedWrites.getOrDefault(index, Collections.emptyMap()).getOrDefault(key, Collections.emptySet());
                for (Long write : writes) {
                    if (write >= txc.xmax || activeTransactions.contains(write)) {
                        valid = false;
                        break;
                    }
                }
            }
        }
        if (valid) {
            for (String index: writtenKeys.keySet()) {
                for (String key : writtenKeys.get(index)) {
                    committedWrites.putIfAbsent(index, new ConcurrentHashMap<>());
                    committedWrites.get(index).putIfAbsent(key, ConcurrentHashMap.newKeySet());
                    committedWrites.get(index).get(key).add(txc.txID);
                }
            }
        }
        validationLock.unlock();
        return valid;
    }

    @Override
    public void commit(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String index: writtenKeys.keySet()) {
            for (String key : writtenKeys.get(index)) {
                lockManager.get(index).get(key).set(false);
            }
        }
    }

    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        // No need to keep track of writes that are visible to all active or future transactions.
        committedWrites.values().forEach(i -> i.values().forEach(w -> w.removeIf(txID -> txID < globalxmin)));
        // Delete old versions that are no longer visible to any active or future transaction.
        try {
            for (String index : committedWrites.keySet()) {
                client.deleteByQuery(dbq -> dbq
                        .index(index)
                        .query(
                                RangeQuery.of(t -> t.field("endVersion").lt(JsonData.of(globalxmin)))._toQuery()
                        )
                );
            }
        } catch (IOException e) {
            // No need to retry on version conflicts, clean up the record in the next GC.
        }
    }
}
