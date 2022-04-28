package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerClient.class);

    protected final ZContext zContext;

    private final Map<String, ZMQ.Socket> sockets = new HashMap<>();

    // A map that stores unique execution ID for each service.
    private final static Map<String, AtomicLong> serviceExecutionIdMap = new ConcurrentHashMap<>();

    public ApiaryWorkerClient() {
        this.zContext = new ZContext();
    }

    public ApiaryWorkerClient(ZContext zContext) {
        this.zContext = zContext;
    }
    
    // This can be used by asynchronous client.
    public ZMQ.Socket getSocket(String address) {
        return this.internalGetSocket(address);
    }

    public static byte[] serializeExecuteRequest(String name, String service, Object... arguments) {
        return internalSerializeExecuteRequest(name, service, getExecutionId(service), 0l, 0, arguments);
    }

    // Synchronous blocking invocation, supposed to be used by client/loadgen.
    public FunctionOutput executeFunction(String address, String name, String service, Object... arguments) throws InvalidProtocolBufferException {
        return internalExecuteFunction(address, name, service, getExecutionId(service), 0l, 0, arguments);
    }

    /* --------------------------- Internal functions ------------------------------- */
    private static long getExecutionId(String service) {
        if (!serviceExecutionIdMap.containsKey(service)) {
            serviceExecutionIdMap.put(service, new AtomicLong(0));
        }
        return serviceExecutionIdMap.get(service).getAndIncrement();
    }

    protected ZMQ.Socket internalGetSocket(String address) {
        if (sockets.containsKey(address)) {
            return sockets.get(address);
        } else {
            ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
            String identity = String.format("%04X-%04X", ThreadLocalRandom.current().nextInt(), ThreadLocalRandom.current().nextInt());
            socket.setIdentity(identity.getBytes(ZMQ.CHARSET));
            socket.connect("tcp://" + address + ":" + ApiaryConfig.workerPort);
            sockets.put(address, socket);
            return socket;
        }
    }

    protected static byte[] internalSerializeExecuteRequest(String name, String service, long execID, long callerID, int taskID, Object... arguments) {
        List<ByteString> byteArguments = new ArrayList<>();
        List<Integer> argumentTypes = new ArrayList<>();
        for (Object o: arguments) {
            if (o instanceof String) {
                String s = (String) o;
                byteArguments.add(ByteString.copyFrom(s.getBytes(StandardCharsets.UTF_8)));
                argumentTypes.add(ApiaryWorker.stringType);
            }  else if (o instanceof Integer) {
                Integer i = (Integer) o;
                byteArguments.add(ByteString.copyFrom(Utilities.toByteArray(i)));
                argumentTypes.add(ApiaryWorker.intType);
            } else {
                assert(o instanceof String[]);
                String[] s = (String[]) o;
                byteArguments.add(ByteString.copyFrom(Utilities.stringArraytoByteArray(s)));
                argumentTypes.add(ApiaryWorker.stringArrayType);
            }
        }
        long sendTime = System.nanoTime();
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder()
                .setName(name)
                .addAllArguments(byteArguments)
                .addAllArgumentTypes(argumentTypes)
                .setCallerId(callerID)
                .setTaskId(taskID)
                .setService(service)
                .setExecutionId(execID)
                .setSenderTimestampNano(sendTime)
                .build();
        return req.toByteArray();
    }

    protected FunctionOutput internalExecuteFunction(String address, String name, String service, long execID, long callerID, int taskID, Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = internalGetSocket(address);
        byte[] reqBytes = internalSerializeExecuteRequest(name, service, execID, callerID, taskID, arguments);
        socket.send(reqBytes, 0);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        return new FunctionOutput(rep.getReplyType() == ApiaryWorker.stringType ? rep.getReplyString() : rep.getReplyInt(), null);
    }

}
