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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static zmq.ZMQ.ZMQ_ROUTER_MANDATORY;
import static zmq.ZMQ.poll;

public class ApiaryWorker {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorker.class);

    public static int stringType = 0;
    public static int stringArrayType = 1;

    private static final int numWorkerThreads = 8;

    private final ApiaryConnection c;
    private ZContext zContext;
    private Thread serverThread;
    private final List<Thread> workerThreads = new ArrayList<>();
    private final Map<String, Callable<StatelessFunction>> statelessFunctions = new HashMap<>();

    public ApiaryWorker(ApiaryConnection c) {
        this.c = c;
        this.zContext = new ZContext();
    }

    private void workerThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ApiaryWorkerClient client = new ApiaryWorkerClient(shadowContext);
        ZMQ.Socket worker = shadowContext.createSocket(SocketType.DEALER);
        worker.connect("inproc://backend");

        List<String> distinctHosts = c.getPartitionHostMap().values().stream()
                                      .distinct()
                                      .collect(Collectors.toList());
        logger.debug("Found {} hosts: {}", distinctHosts.size(), distinctHosts);
        ZMQ.Poller poller = zContext.createPoller(distinctHosts.size() + 1);
        // The backend worker is always the first poller socket.
        poller.register(worker, ZMQ.Poller.POLLIN);
        // Populate sockets for all remote workers in the cluster.
        for (String hostname : distinctHosts) {
            ZMQ.Socket socket = client.getSocket(hostname);
            poller.register(socket, ZMQ.Poller.POLLIN);
        }

        // local variable to store the call stack.
        Map<Integer, ApiaryTaskStash> callerStashMap = new ConcurrentHashMap<>();

        while (!Thread.currentThread().isInterrupted()) {
            int prs = poller.poll(10);
            if (prs == -1) {
                break;
            }

            // Handle request from clients or other workers.
            if (poller.pollin(0)) {
                try {
                    ZMsg msg = ZMsg.recvMsg(worker);
                    ZFrame address = msg.pop();
                    ZFrame content = msg.pop();
                    assert (content != null);
                    msg.destroy();
                    byte[] reqBytes = content.getData();

                    ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(reqBytes);
                    List<ByteString> byteArguments = req.getArgumentsList();
                    List<Integer> argumentTypes = req.getArgumentTypesList();
                    int callerID = req.getCallerId();
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
                    String output = executeFunction(client, req.getName(), callerID, currTaskID, callerStashMap, address, arguments);

                    // If the output is not null, meaning everything is done. Directly return.
                    if (output != null) {
                        ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(output).build();
                        address.send(worker, ZFrame.REUSE + ZFrame.MORE);
                        ZFrame replyContent = new ZFrame(rep.toByteArray());
                        replyContent.send(worker, 0);
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                        break;
                    } else {
                        e.printStackTrace();
                    }
                } catch (InvalidProtocolBufferException e) {
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
                        ZFrame address = msg.pop();
                        ZFrame content = msg.pop();
                        assert (content != null);
                        msg.destroy();
                        byte[] replyBytes = content.getData();
                        ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                        String output = reply.getReply();
                        int callerID = reply.getCallerId();
                        int taskID = reply.getTaskId();

                        // Resume execution.
                        ApiaryTaskStash callerTask = callerStashMap.get(callerID);
                        assert (callerTask != null);
                        callerTask.taskIDtoValue.put(taskID, output);
                        callerTask.finishedTasks.incrementAndGet();
                        processTaskQueue(client, callerTask, callerID);
                        // If everything is resolved, then return the string value.
                        String finalOutput = callerTask.getFinalOutput();
                        if (finalOutput != null) {
                            // Send back the response.
                            ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(finalOutput).build();
                            callerTask.replyAddr.send(socket, ZFrame.REUSE + ZFrame.MORE);
                            ZFrame replyContent = new ZFrame(rep.toByteArray());
                            replyContent.send(worker, 0);
                        }
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                            break;
                        } else {
                            e.printStackTrace();
                        }
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
        poller.close();
        shadowContext.close();
    }

    private void processTaskQueue(ApiaryWorkerClient client, ApiaryTaskStash currTask, int currCallerID) {
        while (!currTask.queuedFunctions.isEmpty()) {
            Task subtask = currTask.queuedFunctions.peek();
            // Run all tasks that have no dependencies.
            try {
                if (subtask.dereferenceFutures(currTask.taskIDtoValue)) {
                    currTask.queuedFunctions.poll();
                    String output;
                    if (statelessFunctions.containsKey(subtask.funcName)) {
                        StatelessFunction f = statelessFunctions.get(subtask.funcName).call();
                        output = f.internalRunFunction(subtask.input);
                        currTask.taskIDtoValue.put(subtask.taskID, output);
                        currTask.finishedTasks.incrementAndGet();
                    } else {
                        String address = c.getHostname(subtask.input);
                        ZMQ.Socket socket = client.getSocket(address);
                        // Async send but do not wait for results.
                        client.sendExecuteRequest(socket, subtask.funcName, currCallerID, subtask.taskID, subtask.input);
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    private String executeFunction(ApiaryWorkerClient client, String name, int callerID, int currTaskID, Map<Integer, ApiaryTaskStash> callerStashMap, ZFrame replyAddr, Object[] arguments) {
        FunctionOutput o = null;
        try {
            o = c.callFunction(name, arguments);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        ApiaryTaskStash currTask = new ApiaryTaskStash(callerID, currTaskID, replyAddr);
        if (o.stringOutput != null) {
            currTask.stringOutput = o.stringOutput;
        } else  {
            assert (o.futureOutput != null);
            currTask.futureOutput = o.futureOutput;
        }

        // Store tasks in the list and async invoke all sub-tasks that are ready.
        // Caller ID to be passed to it's subtasks;
        int currCallerID = ApiaryWorkerClient.callerIDs.getAndIncrement();
        callerStashMap.put(currCallerID, currTask);
        currTask.totalQueuedFunctions = o.calledFunctions.size();
        for (Task subtask : o.calledFunctions) {
            // Queue the task.
            currTask.queuedFunctions.add(subtask);
        }

        processTaskQueue(client, currTask, currCallerID);
        // If everything is resolved, then return the string value.
        return currTask.getFinalOutput();
    }

    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
        frontend.setRouterMandatory(true);
        frontend.bind("tcp://*:" + ApiaryConfig.workerPort);

        ZMQ.Socket backend = shadowContext.createSocket(SocketType.DEALER);
        backend.bind("inproc://backend");

        for (int i = 0; i < numWorkerThreads; i++) {
            Thread t = new Thread(this::workerThread);
            workerThreads.add(t);
            t.start();
        }

        ZMQ.proxy(frontend, backend, null);
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

    public void shutdown() throws InterruptedException {
        for(Thread t: workerThreads) {
            t.interrupt();
            t.join();
        }
        zContext.close();
        serverThread.join();
    }
}
