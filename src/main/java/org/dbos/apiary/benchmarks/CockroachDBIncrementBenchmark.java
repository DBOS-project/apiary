package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;

import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.cockroachdb.CockroachDBConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CockroachDBIncrementBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(IncrementBenchmark.class);

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final int numKeys = 100000;

    public static void benchmark(String cockroachAddr, String service, Integer interval, Integer duration)
            throws IOException, InterruptedException, ProcCallException, SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] { cockroachAddr });
        ds.setPortNumbers(new int[] { ApiaryConfig.cockroachdbPort });
        ds.setDatabaseName("test");
        ds.setUser("root");
        ds.setSsl(false);

        CockroachDBConnection ctxt = new CockroachDBConnection(ds, "KVTable");
        ctxt.deleteEntriesFromTable(/* tableName= */"KVTable");

        Collection<Long> trialTimes = new ConcurrentLinkedQueue<>();
        List<String> distinctHosts = ctxt.getPartitionHostMap().values().stream()
                .distinct()
                .collect(Collectors.toList());


        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);
        ZMQ.Poller poller = clientContext.createPoller(distinctHosts.size());
        for (String hostname : distinctHosts) {
            ZMQ.Socket socket = client.getSocket(hostname);
            poller.register(socket, ZMQ.Poller.POLLIN);
        }
        long startTimeMs = System.currentTimeMillis();
        long endTimeMs = startTimeMs + (duration * 1000 + threadWarmupMs);
        long lastSentTime = System.nanoTime();
        int messagesSent = 0;
        int messagesReceived = 0;
        while (System.currentTimeMillis() < endTimeMs || messagesSent != messagesReceived) {
            if ((System.currentTimeMillis() - startTimeMs) <= threadWarmupMs) {
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

            if (System.currentTimeMillis() < endTimeMs && System.nanoTime() - lastSentTime >= interval * 1000) {
                // Send out a request.
                String key = String.valueOf(ThreadLocalRandom.current().nextInt(numKeys));
                byte[] reqBytes;
                reqBytes = ApiaryWorkerClient.serializeExecuteRequest("IncrementFunction", service, 0, 0, key);
                ZMQ.Socket socket = client.getSocket(ctxt.getHostname(new Object[]{key}));
                socket.send(reqBytes, 0);
                lastSentTime = System.nanoTime();
                messagesSent++;
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTimeMs);
        List<Long> queryTimes = trialTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
        double throughput = (double) numQueries * 1000.0 / elapsedTime;
        long p50 = queryTimes.get(numQueries / 2);
        long p99 = queryTimes.get((numQueries * 99) / 100);
        logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime,
                interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
    }
}
