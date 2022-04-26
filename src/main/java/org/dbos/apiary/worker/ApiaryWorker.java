package org.dbos.apiary.worker;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatelessFunctionContext;
import org.dbos.apiary.interposition.StatelessFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;
import zmq.ZError;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ApiaryWorker {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorker.class);

    public static int stringType = 0;
    public static int stringArrayType = 1;

    private final AtomicLong callerIDs = new AtomicLong(0);
    // Store the call stack for each caller.
    private final Map<Long, ApiaryTaskStash> callerStashMap = new ConcurrentHashMap<>();
    // Store the outgoing messages.
    private final Deque<OutgoingMsg> outgoingReqMsgQueue = new ConcurrentLinkedDeque<>();
    private final Deque<OutgoingMsg> outgoingReplyMsgQueue = new ConcurrentLinkedDeque<>();

    private final ApiaryConnection c;
    private final ApiaryScheduler scheduler;
    private final ZContext zContext = new ZContext(2);  // TODO: How many IO threads?
    private Thread serverThread;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions = new HashMap<>();
    private final ExecutorService reqThreadPool;
    private final ExecutorService statelessReqThreadPool;
    private final ExecutorService repThreadPool;
    private final BlockingQueue<Runnable> reqQueue = new DispatcherPriorityQueue<>();
    private final Set<ZContext> localContexts = ConcurrentHashMap.newKeySet();
    private final Map<String, Deque<Long>> functionRuntimesNs = new ConcurrentHashMap<>();
    private final Map<String, AtomicDouble> functionAverageRuntimesNs = new ConcurrentHashMap<>();
    private final int runningAverageLength = 100;
    private final List<Long> defaultQueue = new ArrayList<>();
    private final Long defaultTimeNs = 100000L;
    private final ThreadLocal<ApiaryWorkerClient> localClients = ThreadLocal.withInitial(() -> {
        ZContext context = ZContext.shadow(zContext);
        localContexts.add(context);
        return new ApiaryWorkerClient(context);
    });

    public ApiaryWorker(ApiaryConnection c, ApiaryScheduler scheduler, int numWorkerThreads) {
        this.c = c;
        this.scheduler = scheduler;
        reqThreadPool = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 0L, TimeUnit.MILLISECONDS, reqQueue);
        statelessReqThreadPool = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 0L, TimeUnit.MILLISECONDS, new DispatcherPriorityQueue<>());
        repThreadPool = new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (int i = 0; i < runningAverageLength; i++) {
            defaultQueue.add(defaultTimeNs);
        }
    }

    private void processQueuedTasks(ApiaryTaskStash currTask, long currCallerID) {
        int numTraversed = 0;
        int totalTasks = currTask.queuedTasks.size();
        while (!currTask.queuedTasks.isEmpty()) {
            try {
                Task subtask = currTask.queuedTasks.peek();
                if (subtask == null) {
                    break;
                }
                // Run all tasks that have no dependencies.
                if (subtask.dereferenceFutures(currTask.taskIDtoValue)) {
                    boolean removed = currTask.queuedTasks.remove(subtask);
                    if (!removed) {
                        continue;
                    }
                    String address = statelessFunctions.containsKey(subtask.funcName) ? c.getPartitionHostMap().get(0) : c.getHostname(subtask.input); // TODO: Fix hack, use local hostname.
                    // Push to the outgoing queue.
                    byte[] reqBytes = ApiaryWorkerClient.serializeExecuteRequest(subtask.funcName, currTask.service, currCallerID, subtask.taskID, subtask.input);
                    outgoingReqMsgQueue.add(new OutgoingMsg(address, reqBytes));
                }
                numTraversed++;
                if (numTraversed >= totalTasks) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    // Resume the execution of the caller function, then send back a reply if everything is finished.
    private void resumeExecution(long callerID, int taskID, String output) throws InterruptedException {
        ApiaryTaskStash callerTask = callerStashMap.get(callerID);
        assert (callerTask != null);
        callerTask.taskIDtoValue.put(taskID, output);
        processQueuedTasks(callerTask, callerID);

        int finishedTasks = callerTask.numFinishedTasks.incrementAndGet();

        // If everything is resolved, then return the string value.
        if (finishedTasks == callerTask.totalQueuedTasks) {
            String finalOutput = callerTask.getFinalOutput();
            assert (finalOutput != null);
            // Send back the response only once.
            ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(finalOutput)
                    .setCallerId(callerTask.callerId)
                    .setTaskId(callerTask.currTaskId)
                    .setSenderTimestampNano(callerTask.senderTimestampNano).build();
            outgoingReplyMsgQueue.add(new OutgoingMsg(callerTask.replyAddr, rep.toByteArray()));

            // Clean up the stash map.
            callerStashMap.remove(callerID);
        }
    }

    // Execute current function, push future tasks into a queue, then send back a reply if everything is finished.
    private void executeFunction(String name, String service, long callerID, int currTaskID, ZFrame replyAddr, long senderTimestampNano, Object[] arguments) throws InterruptedException {
        FunctionOutput o = null;
        long tStart = System.nanoTime();
        try {
            if (!statelessFunctions.containsKey(name)) {
                o = c.callFunction(name, arguments);
            } else {
                StatelessFunction f = statelessFunctions.get(name).call();
                ApiaryFunctionContext ctxt = new ApiaryStatelessFunctionContext(c, localClients.get(), service, statelessFunctions);
                o = f.apiaryRunFunction(ctxt, arguments);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long runtime = System.nanoTime() - tStart;
        assert (o != null);
        ApiaryTaskStash currTask = new ApiaryTaskStash(service, callerID, currTaskID, replyAddr, senderTimestampNano);
        if (o.stringOutput != null) {
            currTask.stringOutput = o.stringOutput;
        } else  {
            assert (o.futureOutput != null);
            currTask.futureOutput = o.futureOutput;
        }

        // Store tasks in the list and async invoke all sub-tasks that are ready.
        // Caller ID to be passed to its subtasks;
        long currCallerID = callerIDs.incrementAndGet();
        currTask.totalQueuedTasks = o.queuedTasks.size();
        // Queue the task.
        currTask.queuedTasks.addAll(o.queuedTasks);

        processQueuedTasks(currTask, currCallerID);
        if (currTask.totalQueuedTasks != currTask.numFinishedTasks.get()) {
            // Need to store the stash map only if we have future tasks. Otherwise, we don't have to store.
            callerStashMap.put(currCallerID, currTask);
        }
        String output = currTask.getFinalOutput();
        // If the output is not null, meaning everything is done. Directly return.
        if (output != null) {
            ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(output)
                    .setCallerId(callerID)
                    .setTaskId(currTaskID)
                    .setSenderTimestampNano(senderTimestampNano).build();
            outgoingReplyMsgQueue.add(new OutgoingMsg(replyAddr, rep.toByteArray()));
        }
        // Record runtime.
        functionRuntimesNs.putIfAbsent(name, new ConcurrentLinkedDeque<>(defaultQueue));
        functionAverageRuntimesNs.putIfAbsent(name, new AtomicDouble((double) defaultTimeNs));
        Deque<Long> times = functionRuntimesNs.get(name);
        times.offerFirst(runtime);
        long old = times.pollLast();
        functionAverageRuntimesNs.get(name).getAndAdd(((double) (runtime - old)) / runningAverageLength);
    }

    private class RequestRunnable implements Runnable, Comparable<RequestRunnable> {
        private ExecuteFunctionRequest req;
        private final ZFrame address;
        public long priority;

        public RequestRunnable(ZFrame address, ExecuteFunctionRequest req) {
            this.address = address;
            try {
                this.req = req;
                long runtime = functionAverageRuntimesNs.getOrDefault(req.getName(), new AtomicDouble(defaultTimeNs)).longValue();
                this.priority = scheduler.getPriority(req.getService(), runtime);
            } catch (AssertionError | Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            // Handle the request.
            try {
                scheduler.onDequeue(req);
                assert (req != null);
                List<ByteString> byteArguments = req.getArgumentsList();
                List<Integer> argumentTypes = req.getArgumentTypesList();
                long callerID = req.getCallerId();
                int currTaskID = req.getTaskId();
                Object[] arguments = new Object[byteArguments.size()];
                for (int i = 0; i < arguments.length; i++) {
                    if (argumentTypes.get(i) == stringType) {
                        arguments[i] = new String(byteArguments.get(i).toByteArray());
                    } else {
                        assert (argumentTypes.get(i) == stringArrayType);
                        arguments[i] = Utilities.byteArrayToStringArray(byteArguments.get(i).toByteArray());
                    }
                }
                executeFunction(req.getName(), req.getService(), callerID, currTaskID, address, req.getSenderTimestampNano(), arguments);
            } catch (AssertionError | Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public int compareTo(RequestRunnable requestRunnable) {
            return Long.compare(priority, requestRunnable.priority);
        }
    }

    private class ReplyRunnable implements Runnable {
        private final byte[] replyBytes;

        public ReplyRunnable(byte[] reply) {
            this.replyBytes = reply;
        }

        @Override
        public void run() {
            // Handle the reply.
            try {
                ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                String output = reply.getReply();
                long callerID = reply.getCallerId();
                int taskID = reply.getTaskId();
                // Resume execution.
                resumeExecution(callerID, taskID, output);
            } catch (InvalidProtocolBufferException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
        // Set high water mark to unbounded, so we can have unlimited outstanding messages.
        // TODO: it may be better to add a bound.
        frontend.setHWM(0);
        frontend.setRouterMandatory(true);
        frontend.bind("tcp://*:" + ApiaryConfig.workerPort);

        // This main server thread is used as I/O thread.
        ApiaryWorkerClient client = new ApiaryWorkerClient(shadowContext);
        List<String> distinctHosts = c.getPartitionHostMap().values().stream()
                .distinct()
                .collect(Collectors.toList());
        ZMQ.Poller poller = zContext.createPoller(distinctHosts.size() + 1);
        // The backend worker is always the first poller socket.
        poller.register(frontend, ZMQ.Poller.POLLIN);
        // Populate sockets for all remote workers in the cluster.
        for (String hostname : distinctHosts) {
            ZMQ.Socket socket = client.getSocket(hostname);
            poller.register(socket, ZMQ.Poller.POLLIN);
        }

        while (!Thread.currentThread().isInterrupted()) {
            // Poll sockets with timeout 1ms.
            // TODO: why poll() is expensive? poll(0) had lower performance.
            int prs = poller.poll(1);
            if (prs == -1) {
                break;
            }

            // Handle request from clients or other workers.
            if (poller.pollin(0)) {
                try {
                    ZMsg msg = ZMsg.recvMsg(frontend);
                    ZFrame address = msg.pop();
                    ZFrame content = msg.poll();
                    assert (content != null);
                    msg.destroy();
                    byte[] reqBytes = content.getData();
                    ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(reqBytes);
                    if (statelessFunctions.containsKey(req.getName())) {
                        statelessReqThreadPool.execute(new RequestRunnable(address, req));
                    } else {
                        reqThreadPool.execute(new RequestRunnable(address, req));
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                        break;
                    } else {
                        e.printStackTrace();
                    }
                } catch (Exception | AssertionError e) {
                    e.printStackTrace();
                }
            }

            // Handle reply from requests.
            for (int i = 1; i < poller.getSize(); i++) {
                if (poller.pollin(i)) {
                    try {
                        String hostname = distinctHosts.get(i-1);
                        ZMQ.Socket socket = client.getSocket(hostname);
                        ZMsg msg = ZMsg.recvMsg(socket);
                        ZFrame content = msg.getLast();
                        assert (content != null);
                        byte[] replyBytes = content.getData();
                        msg.destroy();

                        repThreadPool.execute(new ReplyRunnable(replyBytes));
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                            break;
                        } else {
                            e.printStackTrace();
                        }
                    } catch (Exception | AssertionError e) {
                        e.printStackTrace();
                    }
                }
            }

            // Handle reply to send back.
            // TODO: do we send back all of those, or just send back a few?
            while (!outgoingReplyMsgQueue.isEmpty()) {
                OutgoingMsg msg = outgoingReplyMsgQueue.poll();
                boolean sent;
                try {
                    assert (msg.hostname == null);
                    assert (msg.address != null);
                    sent = msg.address.send(frontend, ZFrame.REUSE | ZFrame.MORE | ZMQ.DONTWAIT);
                    if (!sent) {
                        int errno = frontend.errno();
                        logger.info("Frontend replyAddress failed to send, errno == {}", errno);
                        if (errno != ZError.EAGAIN) {
                            // Ignore the error.
                            continue;
                        } else {
                            outgoingReplyMsgQueue.addFirst(msg);
                            break;
                        }
                    }
                    ZFrame replyContent = new ZFrame(msg.output);
                    sent = replyContent.send(frontend, ZMQ.DONTWAIT);
                    assert (sent);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Continue processing.");
                }
            }

            while (!outgoingReqMsgQueue.isEmpty()) {
                OutgoingMsg msg = outgoingReqMsgQueue.poll();
                boolean sent;
                try {
                    assert  (msg.hostname != null);
                    ZMQ.Socket socket = client.getSocket(msg.hostname);
                    sent = socket.send(msg.output, ZMQ.DONTWAIT);
                    if (!sent) {
                        // Something went wrong.
                        int errno = socket.errno();
                        logger.info("Socket Failed to send, errno == {}", errno);
                        if (errno != ZError.EAGAIN) {
                            // Ignore the error.
                            continue;
                        } else {
                            outgoingReqMsgQueue.addFirst(msg);
                            break;
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    logger.info("Continue processing.");
                }
            }
        }

        poller.close();
        shadowContext.close();
    }

    // TODO: Can registration be centralized instead of doing it on every worker separately?
    public void registerStatelessFunction(String name, Callable<StatelessFunction> function) {
        statelessFunctions.put(name, function);
    }

    public void startServing() {
        serverThread = new Thread(this::serverThread);
        serverThread.start();
    }

    public void shutdown() {
        try {
            reqThreadPool.shutdown();
            reqThreadPool.awaitTermination(10000, TimeUnit.SECONDS);
            repThreadPool.shutdown();
            repThreadPool.awaitTermination(10000, TimeUnit.SECONDS);
            statelessReqThreadPool.shutdown();
            statelessReqThreadPool.awaitTermination(10000, TimeUnit.SECONDS);
            localContexts.forEach(ZContext::close);
            serverThread.interrupt();
            zContext.close();
            serverThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
