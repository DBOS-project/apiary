package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RetwisBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RetwisBenchmark.class);

    public static final int numPosts = 5000;
    public static final int numUsers = 1000;
    public static final int followsPerUsers = 50;
    public static final int percentageGetTimeline = 90;
    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final int threadPoolSize = 256;
    private static final int numThreads = 1;

    public static void benchmark(String voltAddr, String role, Integer interval, Integer duration) throws IOException, InterruptedException, ProcCallException {
        VoltConnection conn = new VoltConnection(voltAddr, ApiaryConfig.voltdbPort);
        conn.client.callProcedure("TruncateTables");

        ZContext loadClientContext = new ZContext();
        ThreadLocal<ApiaryWorkerClient> loadClient = ThreadLocal.withInitial(() -> new ApiaryWorkerClient(voltAddr, ZContext.shadow(loadClientContext)));
        Collection<Long> trialTimes = new ConcurrentLinkedQueue<>();

        AtomicInteger timestamp = new AtomicInteger(0);
        AtomicInteger postIDs = new AtomicInteger(0);
        ExecutorService retwisPool = Executors.newFixedThreadPool(threadPoolSize);
        CountDownLatch latch = new CountDownLatch(numPosts + numUsers * followsPerUsers);
        for (int i = 0; i < numPosts; i++) {
            Runnable r = () -> {
                try {
                    int userID = ThreadLocalRandom.current().nextInt(numUsers);
                    int postID = postIDs.incrementAndGet();
                    int ts = timestamp.incrementAndGet();
                    String postString = String.format("matei%d", postID);
                    loadClient.get().executeFunction("RetwisPost", userID, postID, ts, postString);
                    latch.countDown();
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            };
            retwisPool.submit(r);
        }
        for (int userID = 0; userID < numUsers; userID++) {
            int firstFollowee = ThreadLocalRandom.current().nextInt(numUsers);
            for (int i = 0; i < followsPerUsers; i++) {
                int finalI = i;
                int finalUserID = userID;
                Runnable r = () ->  {
                    try {
                        int followeeID = (firstFollowee + finalI) % numUsers;
                        loadClient.get().executeFunction("RetwisFollow", finalUserID, followeeID);
                        latch.countDown();
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                };
                retwisPool.submit(r);
            }
        }
        latch.await();
        logger.info("Finished loading!");
        retwisPool.shutdown();
        retwisPool.awaitTermination(100000, TimeUnit.SECONDS);

        List<String> distinctHosts = conn.getPartitionHostMap().values().stream()
                .distinct()
                .collect(Collectors.toList());
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        List<Thread> threads = new ArrayList<>();
        long threadInterval = interval.longValue() * numThreads;

        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            Runnable threadRunnable = () -> {
                ZContext clientContext = new ZContext();
                ApiaryWorkerClient client = new ApiaryWorkerClient(voltAddr, clientContext);
                ZMQ.Poller poller = clientContext.createPoller(distinctHosts.size());
                for (String hostname : distinctHosts) {
                    ZMQ.Socket socket = client.getSocket(hostname);
                    poller.register(socket, ZMQ.Poller.POLLIN);
                }
                long lastSentTime = System.nanoTime();
                int messagesSent = 0;
                int messagesReceived = 0;
                while (System.currentTimeMillis() < endTime || messagesSent != messagesReceived) {
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
                                ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                                long senderTs = reply.getSenderTimestampNano();
                                trialTimes.add(System.nanoTime() - senderTs);
                                messagesReceived++;
                                if (messagesSent < messagesReceived) {
                                    logger.info("sent {}, received {}", messagesSent, messagesReceived);
                                    logger.info("sendTime {}, callerID {}", reply.getSenderTimestampNano(), reply.getCallerId());
                                }

                            } catch (ZMQException e) {
                                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                                    e.printStackTrace();
                                    break;
                                } else {
                                    e.printStackTrace();
                                }
                            } catch (InvalidProtocolBufferException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    if (System.currentTimeMillis() < endTime && System.nanoTime() - lastSentTime >= threadInterval * 1000) {
                        // Send out a request.
                        lastSentTime = System.nanoTime();
                        int userID = ThreadLocalRandom.current().nextInt(numUsers);
                        byte[] reqBytes = client.serializeExecuteRequest("RetwisGetTimeline", role, userID);
                        ZMQ.Socket socket = client.getSocket(conn.getHostname(String.valueOf(userID)));
                        socket.send(reqBytes, 0);
                        messagesSent++;
                    }
                }
            };
            Thread t = new Thread(threadRunnable);
            threads.add(t);
            t.start();
        }

        for (Thread t: threads) {
            t.join();
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;
        List<Long> queryTimes = trialTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
        double throughput = (double) numQueries * 1000.0 / elapsedTime;
        long p50 = queryTimes.get(numQueries / 2);
        long p99 = queryTimes.get((numQueries * 99) / 100);
        logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
    }
}
