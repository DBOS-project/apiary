package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.UpdateByQueryResponse;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ElasticsearchConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConnection.class);
    public ElasticsearchClient client;

    private final Map<String, Set<Long>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();

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
        } catch (CertificateException | IOException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            logger.info("Elasticsearch Connection Failed");
            throw new RuntimeException("Failed to connect to ElasticSearch");
        }
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Object... inputs) throws Exception {
        ApiaryContext ctxt = new ElasticsearchContext(client, workerContext, txc, service, execID, functionID);
        FunctionOutput f = null;
        try {
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return f;
    }

    @Override
    public boolean validate(List<String> writtenKeys, TransactionContext txc) {
        Set<Long> activeTransactions = Arrays.stream(txc.activeTransactions).boxed().collect(Collectors.toSet());
        validationLock.lock();
        boolean valid = true;
        for (String key: writtenKeys) {
            // Has the key been modified by a transaction not in the snapshot?
            Set<Long> writes = committedWrites.getOrDefault(key, Collections.emptySet());
            for (Long write: writes) {
                if (write >= txc.xmax || activeTransactions.contains(write)) {
                    valid = false;
                    break;
                }
            }
        }
        if (valid) {
            for (String key: writtenKeys) {
                committedWrites.putIfAbsent(key, ConcurrentHashMap.newKeySet());
                committedWrites.get(key).add(txc.txID);
            }
        }
        validationLock.unlock();
        try {
            String updateScript = String.format("ctx._source.endVersion=%s", txc.txID);
            if (valid) {
                for (String key : writtenKeys) {
                    UpdateByQueryResponse r = client.updateByQuery(ubq -> ubq
                            .index("people") // TODO: Don't hardcode.
                            .query(BoolQuery.of(bb -> bb
                                            .must(
                                                    MatchQuery.of(t -> t.field("apiaryID").query(key))._toQuery(),
                                                    RangeQuery.of(f -> f.field("beginVersion").lt(JsonData.of(txc.txID)))._toQuery()
                                            )
                            )._toQuery())
                            .script(s -> s
                                    .inline(InlineScript.of(i -> i.lang("painless").source(updateScript)))
                            )
                            .refresh(Boolean.TRUE)
                    );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return valid;
    }

    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        committedWrites.values().forEach(w -> w.removeIf(txID -> txID < globalxmin));
    }
}
