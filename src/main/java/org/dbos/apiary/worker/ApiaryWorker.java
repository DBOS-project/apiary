package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;
import org.dbos.apiary.introspect.PartitionInfo;
import org.dbos.apiary.stateless.StatelessFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class ApiaryWorker {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorker.class);

    public static int stringType = 0;
    public static int stringArrayType = 1;

    private static final int numWorkerThreads = 128;

    private final ApiaryConnection c;
    private ZContext zContext;
    private Thread serverThread;
    private final List<Thread> workerThreads = new ArrayList<>();
    private final PartitionInfo partitionInfo;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions = new HashMap<>();

    public ApiaryWorker(ApiaryConnection c, PartitionInfo partitionInfo) {
        this.c = c;
        this.partitionInfo = partitionInfo;
        this.zContext = new ZContext();
    }

    private void workerThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ApiaryWorkerClient client = new ApiaryWorkerClient(shadowContext);
        ZMQ.Socket worker = shadowContext.createSocket(SocketType.REP);
        worker.connect("inproc://backend");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] reqBytes = worker.recv(0);
                ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(reqBytes);
                List<ByteString> byteArguments = req.getArgumentsList();
                List<Integer> argumentTypes = req.getArgumentTypesList();
                Object[] arguments = new Object[byteArguments.size()];
                for (int i = 0; i < arguments.length; i++) {
                    if (argumentTypes.get(i) == stringType) {
                        arguments[i] = new String(byteArguments.get(i).toByteArray());
                    } else {
                        assert (argumentTypes.get(i) == stringArrayType);
                        arguments[i] = Utilities.byteArrayToStringArray(byteArguments.get(i).toByteArray());
                    }
                }
                String output = executeFunction(client, req.getName(), req.getPkey(), arguments);
                assert output != null;
                ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(output).build();
                worker.send(rep.toByteArray());
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
        shadowContext.close();
    }

    private String executeFunction(ApiaryWorkerClient client, String name, int pkey, Object[] arguments) {
        try {
            FunctionOutput o = c.callFunction(name, pkey, arguments);
            Map<Integer, String> taskIDtoValue = new ConcurrentHashMap<>();
            for (Task task: o.calledFunctions) {
                task.dereferenceFutures(taskIDtoValue);
                String output;
                if (statelessFunctions.containsKey(task.funcName)) {
                    StatelessFunction f = statelessFunctions.get(task.funcName).call();
                    output = f.internalRunFunction(task.input);
                } else {
                    String address = partitionInfo.getHostname(task.pkey);
                    output = client.executeFunction(address, task.funcName, task.pkey, task.input);
                }
                taskIDtoValue.put(task.taskID, output);
            }
            if (o.stringOutput != null) {
                return o.stringOutput;
            } else {
                assert o.futureOutput != null;
                return taskIDtoValue.get(o.futureOutput.futureID);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
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
