package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ApiaryWFQScheduler implements ApiaryScheduler {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWFQScheduler.class);

    Map<String, AtomicLong> activeCount = new ConcurrentHashMap<>();

    @Override
    public long getPriority(String service, long runtime) {
        activeCount.putIfAbsent(service, new AtomicLong(0));
        return System.nanoTime() + runtime * activeCount.get(service).incrementAndGet();
    }

    @Override
    public void onDequeue(ExecuteFunctionRequest req) {
        String service = req.getService();
        activeCount.get(service).decrementAndGet();
    }
}
