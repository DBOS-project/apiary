package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class IncrementBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(IncrementBenchmark.class);

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final int threadPoolSize = 128;
    private static final Collection<Long> trialTimes = new ConcurrentLinkedQueue<>();

    static class CallbackRunnable implements Runnable {
        private final byte[] replyBytes;

        public CallbackRunnable(byte[] replyBytes) {
            this.replyBytes = replyBytes;
        }

        @Override
        public void run() {
            try {
                ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                long senderTs = reply.getSenderTimestampNano();
                trialTimes.add(System.nanoTime() - senderTs);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

    public static void benchmark(String voltAddr, Integer interval, Integer duration) throws IOException, InterruptedException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection(voltAddr, ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(ZContext.shadow(clientContext));


        final int numKeys = 100000;

        List<String> distinctHosts = ctxt.getPartitionHostMap().values().stream()
                .distinct()
                .collect(Collectors.toList());
        ZMQ.Poller poller = clientContext.createPoller(distinctHosts.size());
        for (String hostname : distinctHosts) {
            ZMQ.Socket socket = client.getSocket(hostname);
            poller.register(socket, ZMQ.Poller.POLLIN);
        }

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

        long lastSentTime = System.nanoTime();
        while (System.currentTimeMillis() < endTime) {
            if ((System.currentTimeMillis() - startTime) <= threadWarmupMs) {
                // Clean up the arrays if still in warm up time.
                trialTimes.clear();
            }
            int prs = poller.poll(0);
            if (prs == -1) {
                break;
            }

            // Handle reply from server.
            for (int i = 0; i < poller.getSize(); i++) {
                if (poller.pollin(i)) {
                    try {
                        String hostname = distinctHosts.get(i);
                        ZMQ.Socket socket = client.getSocket(hostname);
                        ZMsg msg = ZMsg.recvMsg(socket);
                        ZFrame content = msg.getLast();
                        assert (content != null);
                        byte[] replyBytes = content.getData();
                        msg.destroy();
                        threadPool.submit(new CallbackRunnable(replyBytes));
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                            break;
                        } else {
                            e.printStackTrace();
                        }
                    }
                }
            }

            if (System.nanoTime() - lastSentTime >= interval.longValue() * 1000) {
                // Send out a request.
                String key = String.valueOf(ThreadLocalRandom.current().nextInt(numKeys));
                byte[] reqBytes = ApiaryWorkerClient.getExecuteRequestBytes("IncrementProcedure", 0, 0, key);
                ZMQ.Socket socket = client.getSocket(ctxt.getHostname(new Object[]{key}));
                socket.send(reqBytes, 0);
                lastSentTime = System.nanoTime();
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;
        List<Long> queryTimes = trialTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
        double throughput = (double) numQueries * 1000.0 / elapsedTime;
        long p50 = queryTimes.get(numQueries / 2);
        long p99 = queryTimes.get((numQueries * 99) / 100);
        logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
    }
}
