package org.dbos.apiary;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchSearchPerson;
import org.dbos.apiary.procedures.postgres.tests.PostgresFibSumFunction;
import org.dbos.apiary.procedures.postgres.tests.PostgresFibonacciFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElasticsearchTests {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTests.class);

    private ApiaryWorker apiaryWorker;

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @BeforeEach
    public void cleanupElasticsearch() {
        try {
            ElasticsearchClient client = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("people").build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
    }

    @Test
    public void testElasticsearchBasic() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testElasticsearchBasic");

        ElasticsearchConnection conn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
        } catch (Exception e) {
            logger.info("No Elasticsearch instance!");
            return;
        }
        conn.registerFunction("ElasticsearchIndexPerson", ElasticsearchIndexPerson::new);
        conn.registerFunction("ElasticsearchSearchPerson", ElasticsearchSearchPerson::new);

        apiaryWorker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 4);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("ElasticsearchIndexPerson", "matei", 1).getInt();
        assertEquals(1, res);

        Thread.sleep(5000);

        res = client.executeFunction("ElasticsearchSearchPerson", "matei").getInt();
        assertEquals(1, res);
    }
}
