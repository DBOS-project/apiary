package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.StoredScriptId;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticsearchContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchContext.class);

    private final ElasticsearchClient client;
    private final TransactionContext txc;

    public final Map<String, List<String>> writtenKeys;
    private final Map<String, Map<String, AtomicBoolean>> lockManager;

    public ElasticsearchContext(ElasticsearchClient client, Map<String, List<String>> writtenKeys, Map<String, Map<String, AtomicBoolean>> lockManager, WorkerContext workerContext, TransactionContext txc, String role, long execID, long functionID) {
        super(workerContext, role, execID, functionID, ApiaryConfig.ReplayMode.NOT_REPLAY.getValue());
        this.client = client;
        this.txc = txc;
        this.lockManager = lockManager;
        this.writtenKeys = writtenKeys;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        // TODO: Implement.
        return null;
    }

    public void executeWrite(String index, ApiaryDocument document, String id) throws IOException, PSQLException {
        if (!ApiaryConfig.XDBTransactions) {
            IndexRequest.Builder b = new IndexRequest.Builder().index(index).document(document);
            b = b.id(id);
            client.index(b.build());
            client.indices().refresh(r -> r.index(index));
            return;
        }
        lockManager.putIfAbsent(index, new ConcurrentHashMap<>());
        lockManager.get(index).putIfAbsent(id, new AtomicBoolean(false));
        boolean available = lockManager.get(index).get(id).compareAndSet(false, true);
        if (!available) {
            throw new PSQLException("tuple locked", PSQLState.SERIALIZATION_FAILURE);
        }
        document.setApiaryID(id);
        document.setBeginVersion(txc.txID);
        document.setEndVersion(Long.MAX_VALUE);
        IndexRequest.Builder b = new IndexRequest.Builder().index(index).document(document);
        client.index(b.build());
        client.updateByQuery(ubq -> ubq
                .index(index)
                .query(BoolQuery.of(bb -> bb
                        .must(
                                MatchQuery.of(t -> t.field("apiaryID").query(id))._toQuery(),
                                RangeQuery.of(f -> f.field("beginVersion").lt(JsonData.of(txc.txID)))._toQuery(),
                                TermQuery.of(t -> t.field("endVersion").value(Long.MAX_VALUE))._toQuery()
                        )
                )._toQuery())
                .script(s -> s
                        .stored(StoredScriptId.of(ss -> ss
                                .id(ElasticsearchConnection.updateEndVersionScript).params(Map.of("endV", JsonData.of(txc.txID))))
                        )
                )
        );
        client.indices().refresh(r -> r.index(index));
        writtenKeys.putIfAbsent(index, new ArrayList<>());
        writtenKeys.get(index).add(id);
    }

    public void executeBulkWrite(String index, List<ApiaryDocument> documents, List<String> ids) throws IOException {
        assert(documents.size() == ids.size());
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (int i = 0; i < documents.size(); i++) {
            ApiaryDocument document = documents.get(i);
            String id = ids.get(i);
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
        client.indices().refresh(r -> r.index(index));
        logger.info("Bulk load: {} {}", documents.size(), rr.errors());
    }

    public SearchResponse executeQuery(String index, Query searchQuery, Class clazz) throws IOException {
        if (!ApiaryConfig.XDBTransactions) {
            SearchRequest request = SearchRequest.of(s -> s
                    .index(index).query(searchQuery));
            return client.search(request, clazz);
        }
        List<Query> mustNotFilter = new ArrayList<>();
        // If beginVersion is an active transaction, it is not in the snapshot.
        for (long txID : txc.activeTransactions) {
            mustNotFilter.add(TermQuery.of(f -> f.field("beginVersion").value(txID))._toQuery());
        }
        mustNotFilter.add(TermQuery.of(t -> t.field("endVersion").value(txc.txID))._toQuery());
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
                                        BoolQuery.of(m -> m
                                                        .should(
                                                                RangeQuery.of(f -> f.field("beginVersion").lt(JsonData.of(txc.xmax)))._toQuery(),
                                                                TermQuery.of(t -> t.field("beginVersion").value(txc.txID))._toQuery()
                                                        ).minimumShouldMatch("1")
                                        )._toQuery()
                                ).mustNot( // Therefore, beginVersion must not be an active transaction.
                                        mustNotFilter
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
        return rr;
    }
}
