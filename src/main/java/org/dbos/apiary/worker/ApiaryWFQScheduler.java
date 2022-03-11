package org.dbos.apiary.worker;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ApiaryWFQScheduler implements ApiaryScheduler {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWFQScheduler.class);

    private final ApiaryConnection c;
    private final Map<Integer, WFQueue> partitionQueues = new HashMap<>();
    private final Map<WFQTask, FunctionOutput> outputs = new HashMap<>();
    private final List<Thread> partitionThreads = new ArrayList<>();

    private boolean serving = true;

    public ApiaryWFQScheduler(ApiaryConnection c, List<Integer> localPartitions) {
        this.c = c;
        for (Integer p : localPartitions) {
            WFQueue queue = new WFQueue();
            partitionQueues.put(p, queue);
            Runnable r = () -> {
                partitionThread(queue);
            };
            Thread t = new Thread(r);
            partitionThreads.add(t);
            t.start();
        }
    }

    public void partitionThread(WFQueue queue) {
        while(serving) {
            WFQTask task = queue.dequeue();
            if (task != null) {
                try {
                    FunctionOutput fo = c.callFunction(task.name, task.inputs);
                    outputs.put(task, fo);
                    task.latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void shutdown() {
        serving = false;
        for (Thread t: partitionThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public FunctionOutput scheduleFunction(String name, Object... inputs) {
        try {
            String service = name.substring(0, 2);
            CountDownLatch latch = new CountDownLatch(1);
            WFQTask task = new WFQTask(name, inputs, latch, service);
            int partition = c.getPartition(inputs);
            partitionQueues.get(partition).enqueue(task);
            latch.await();
            FunctionOutput output = outputs.get(task);
            outputs.remove(task);
            return output;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static class WFQueue {
        private static final long taskLengthNanos = 10000L;
        Map<String, AtomicLong> activeCount = new HashMap<>();
        Queue<WFQTask> queue = new PriorityBlockingQueue<>();

        public void enqueue(WFQTask task) {
            activeCount.putIfAbsent(task.service, new AtomicLong(0L));
            task.virtualFinishTime = System.nanoTime() + taskLengthNanos * activeCount.get(task.service).incrementAndGet();
            queue.add(task);
        }

        public WFQTask dequeue() {
            WFQTask task = queue.poll();
            if (task != null) {
                activeCount.get(task.service).decrementAndGet();
            }
            return task;
        }
    }

    private static class WFQTask implements Comparable<WFQTask> {
        final String name;
        final Object[] inputs;
        final CountDownLatch latch;
        final String service;

        long virtualFinishTime;

        public WFQTask(String name, Object[] inputs, CountDownLatch latch, String service) {
            this.name = name;
            this.inputs = inputs;
            this.latch = latch;
            this.service = service;
        }

        @Override
        public int compareTo(WFQTask wfqTask) {
            return Long.compare(virtualFinishTime, wfqTask.virtualFinishTime);
        }
    }
}
