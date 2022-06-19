package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.core.search.Profile;
import co.elastic.clients.elasticsearch.core.search.QueryProfile;
import co.elastic.clients.elasticsearch.core.search.SearchProfile;
import co.elastic.clients.elasticsearch.core.search.ShardProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class ElasticsearchUtilities {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchContext.class);

    public static void printESProfile(Profile profile) {
        for (ShardProfile p: profile.shards()) {
            for (SearchProfile s: p.searches()) {
                Stack<QueryProfile> stack = new Stack<>();
                s.query().forEach(stack::push);
                while (!stack.empty()) {
                    QueryProfile q = stack.pop();
                    logger.info("{} {} Runtime: {}μs", q.type(), q.description(), q.timeInNanos() / 1000);
                    q.children().forEach(stack::push);
                }
            }
        }
    }
}
