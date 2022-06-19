package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchContext.class);

    private final ElasticsearchClient client;
    private final TransactionContext txc;

    Map<String, List<String>> writtenKeys = new HashMap<>();

    public ElasticsearchContext(ElasticsearchClient client, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.client = client;
        this.txc = txc;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        // TODO: Implement.
        return null;
    }

    public void executeWrite(String index, ApiaryDocument document, String id) {
        try {
            long t0 = System.nanoTime();
            writtenKeys.putIfAbsent(index, new ArrayList<>());
            writtenKeys.get(index).add(id);
            if (ApiaryConfig.XDBTransactions) {
                document.setApiaryID(id);
                document.setBeginVersion(txc.txID);
                document.setEndVersion(Long.MAX_VALUE);
            }
            IndexRequest.Builder b = new IndexRequest.Builder().index(index).document(document);
            if (!ApiaryConfig.XDBTransactions) {
                b = b.id(id);
            }
            client.index(b.build());
            logger.debug("Write: {}", (System.nanoTime() - t0) / 1000L);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void executeBulkWrite(String index, List<ApiaryDocument> documents, List<String> ids) throws IOException {
        assert(documents.size() == ids.size());
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (int i = 0; i < documents.size(); i++) {
            ApiaryDocument document = documents.get(i);
            String id = ids.get(i);
            writtenKeys.putIfAbsent(index, new ArrayList<>());
            writtenKeys.get(index).add(id);
            document.setApiaryID(id);
            document.setBeginVersion(txc.txID);
            document.setEndVersion(Long.MAX_VALUE);
            br.operations(op -> op
                .index(idx -> idx
                        .index(index)
                        .document(document)
                )
            );
        }
        BulkResponse rr = client.bulk(br.build());
        logger.info("Bulk load: {} {}", documents.size(), rr.errors());
    }

    public SearchResponse executeQuery(String index, Query searchQuery, Class clazz) {
        try {
            if (ApiaryConfig.XDBTransactions) {
                long t0 = System.nanoTime();
                List<Query> beginVersionFilter = new ArrayList<>();
                // If beginVersion is an active transaction, it is not in the snapshot.
                for (long txID : txc.activeTransactions) {
                    beginVersionFilter.add(TermQuery.of(f -> f.field("beginVersion").value(txID))._toQuery());
                }
                List<Query> endVersionFilter = new ArrayList<>();
                // If endVersion is greater than or equal to xmax, it is not in the snapshot.
                endVersionFilter.add(RangeQuery.of(f -> f.field("endVersion").gte(JsonData.of(txc.xmax)))._toQuery());
                // If endVersion is an active transaction, it is not in the snapshot.
                for (long txID : txc.activeTransactions) {
                    endVersionFilter.add(TermQuery.of(f -> f.field("endVersion").value(txID))._toQuery());
                }
                SearchRequest request = SearchRequest.of(s -> s
                        .index(index).query(q -> q.bool(b -> b
                                .must(searchQuery)
                                .filter(BoolQuery.of(bb -> bb
                                        .must( // beginVersion must be in the snapshot.
                                                RangeQuery.of(f -> f.field("beginVersion").lt(JsonData.of(txc.xmax)))._toQuery()
                                        ).mustNot( // Therefore, beginVersion must not be an active transaction.
                                                beginVersionFilter
                                        ).should( // endVersion must not be in the snapshot.
                                                endVersionFilter
                                        )
                                        .minimumShouldMatch("1")
                                )._toQuery())
                        )).profile(ApiaryConfig.profile)
                );
                SearchResponse rr =  client.search(request, clazz);
                if (ApiaryConfig.profile) {
                    ElasticsearchUtilities.printESProfile(rr.profile());
                }
                logger.debug("Read: {} {}", (System.nanoTime() - t0) / 1000L, txc.activeTransactions.size());
                return rr;
            } else {
                SearchRequest request = SearchRequest.of(s -> s
                        .index(index).query(searchQuery));
                return client.search(request, clazz);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
