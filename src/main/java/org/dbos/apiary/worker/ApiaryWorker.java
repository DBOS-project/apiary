package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;
import org.dbos.apiary.stateless.StatelessFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

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
    private final Queue<OutgoingReplyMsg> outgoingReplyMsgs = new ConcurrentLinkedQueue<>();
    private final Queue<OutgoingRequestMsg> outgoingRequestMsgs = new ConcurrentLinkedQueue<>();

    public static int numWorkerThreads = 128;

    private final ApiaryConnection c;
    private ZContext zContext;
    private Thread[] serverThreads;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions = new HashMap<>();
    private final ExecutorService threadPool;

    public ApiaryWorker(ApiaryConnection c) {
        this.c = c;
        this.zContext = new ZContext(2);  // TODO: How many IO threads?
        threadPool = Executors.newFixedThreadPool(numWorkerThreads);
    }

    private void processQueuedTasks(ApiaryTaskStash currTask, long currCallerID) throws InterruptedException {
        boolean isLockAcquired = currTask.queuedTasksLock.tryLock(1, TimeUnit.MILLISECONDS);
        while (isLockAcquired && !currTask.queuedTasks.isEmpty()) {
            try {
                Task subtask = currTask.queuedTasks.peek();
                // Run all tasks that have no dependencies.
                if (subtask.dereferenceFutures(currTask.taskIDtoValue)) {
                    currTask.queuedTasks.poll();
                    String output;
                    if (statelessFunctions.containsKey(subtask.funcName)) {
                        StatelessFunction f = statelessFunctions.get(subtask.funcName).call();
                        output = f.internalRunFunction(subtask.input);
                        currTask.taskIDtoValue.put(subtask.taskID, output);
                        currTask.numFinishedTasks.incrementAndGet();
                    } else {
                        String address = c.getHostname(subtask.input);
                        // Push to the outgoing queue.
                        byte[] reqBytes = ApiaryWorkerClient.getExecuteRequestBytes(subtask.funcName, currCallerID, subtask.taskID, subtask.input);
                        outgoingRequestMsgs.add(new OutgoingRequestMsg(address, reqBytes));
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        currTask.queuedTasksLock.unlock();
    }

    // Resume the execution of the caller function, then send back a reply if everything is finished.
    private void resumeExecution(long callerID, int taskID, String output) throws InterruptedException {
        ApiaryTaskStash callerTask = callerStashMap.get(callerID);
        assert (callerTask != null);
        callerTask.taskIDtoValue.put(taskID, output);
        callerTask.numFinishedTasks.incrementAndGet();
        processQueuedTasks(callerTask, callerID);

        // If everything is resolved, then return the string value.
        String finalOutput = callerTask.getFinalOutput();
        if (finalOutput != null) {
            // Send back the response.
            ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(finalOutput)
                    .setCallerId(callerTask.callerId)
                    .setTaskId(callerTask.currTaskId).build();
            outgoingReplyMsgs.add(new OutgoingReplyMsg(callerTask.replyAddr, rep.toByteArray()));
            // Clean up the stash map.
            callerStashMap.remove(callerID);
        }
    }

    // Execute current function, push future tasks into a queue, then send back a reply if everything is finished.
    private void executeFunction(String name, long callerID, int currTaskID, ZFrame replyAddr, Object[] arguments) throws InterruptedException {
        FunctionOutput o;
        try {
            o = c.callFunction(name, arguments);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        ApiaryTaskStash currTask = new ApiaryTaskStash(callerID, currTaskID, replyAddr);
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
        for (Task subtask : o.queuedTasks) {
            // Queue the task.
            currTask.queuedTasks.add(subtask);
        }

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
                    .setTaskId(currTaskID).build();
            outgoingReplyMsgs.add(new OutgoingReplyMsg(replyAddr, rep.toByteArray()));
        }
    }

    class workerRunnable implements Runnable {
        private final byte[] reqBytes;
        private final byte[] replyBytes;
        private final ZFrame address;

        public workerRunnable(ZFrame address, byte[] req, byte[] reply) {
            this.address = address;
            this.reqBytes = req;
            this.replyBytes = reply;
        }

        @Override
        public void run() {
            if (reqBytes != null) {
                // Handle the request.
                try {
                    ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(reqBytes);
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
                    executeFunction(req.getName(), callerID, currTaskID, address, arguments);
                } catch (InvalidProtocolBufferException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                assert (replyBytes != null);
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
    }

    // This client hanlde thread is used as I/O thread for receiving replies and sending requests.
    private void clientHandleThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ApiaryWorkerClient client = new ApiaryWorkerClient(shadowContext);

        List<String> distinctHosts = c.getPartitionHostMap().values().stream()
                .distinct()
                .collect(Collectors.toList());
        ZMQ.Poller poller = zContext.createPoller(distinctHosts.size());
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

            // Handle reply from requests.
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
                        threadPool.submit(new workerRunnable(null, null, replyBytes));
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                            break;
                        } else {
                            e.printStackTrace();
                        }
                    }
                }
            }

            // Handle request to send out.
            // TODO: do we send back all of those, or just send back a few?
            while (!outgoingRequestMsgs.isEmpty()) {
                OutgoingRequestMsg msg = outgoingRequestMsgs.poll();
                ZMQ.Socket socket = client.getSocket(msg.hostname);
                socket.send(msg.output, 0);
            }
        }

        poller.close();
        shadowContext.close();
    }

    // This main server thread is used as I/O thread for receiving requests and sending back replies.
    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
        frontend.setRouterMandatory(true);
        frontend.bind("tcp://*:" + ApiaryConfig.workerPort);

        while (!Thread.currentThread().isInterrupted()) {
            // Handle request from clients or other workers.
            try {
                ZMsg msg = ZMsg.recvMsg(frontend, ZMQ.DONTWAIT);
                if (msg != null) {
                    ZFrame address = msg.pop();
                    ZFrame content = msg.poll();
                    assert (content != null);
                    msg.destroy();
                    byte[] reqBytes = content.getData();
                    threadPool.submit(new workerRunnable(address, reqBytes, null));
                }
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                    break;
                } else {
                    e.printStackTrace();
                }
            }

            // Handle reply to send back.
            // TODO: do we send back all of those, or just send back a few?
            while (!outgoingReplyMsgs.isEmpty()) {
                OutgoingReplyMsg msg = outgoingReplyMsgs.poll();
                assert (msg.address != null);
                msg.address.send(frontend, ZFrame.REUSE + ZFrame.MORE);
                ZFrame replyContent = new ZFrame(msg.output);
                replyContent.send(frontend, 0);
            }
        }
        shadowContext.close();
    }

    // TODO: Can registration be centralized instead of doing it on every worker separately?
    public void registerStatelessFunction(String name, Callable<StatelessFunction> function) {
        statelessFunctions.put(name, function);
    }

    public void startServing() {
        serverThreads = new Thread[2];
        serverThreads[0] = new Thread(this::serverThread);
        serverThreads[0].start();
        serverThreads[1] = new Thread(this::clientHandleThread);
        serverThreads[1].start();
    }

    public void shutdown() throws InterruptedException {
        threadPool.shutdown();
        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        for (int i = 0; i < serverThreads.length; i++) {
            serverThreads[i].interrupt();
            serverThreads[i].join();
        }
        zContext.close();
    }
}
