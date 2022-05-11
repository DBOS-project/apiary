package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class InternalApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(InternalApiaryWorkerClient.class);

    protected final ZContext zContext;

    private final Map<String, ZMQ.Socket> sockets = new HashMap<>();

    public InternalApiaryWorkerClient(ZContext zContext) {
        this.zContext = zContext;
    }

    // This can be used by asynchronous client.
    public ZMQ.Socket getSocket(String address) {
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

    public static byte[] serializeExecuteRequest(String name, String service, long execID, long callerID, long functionID, Object... arguments) {
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
            }  else if (o instanceof String[]) {
                String[] s = (String[]) o;
                byteArguments.add(ByteString.copyFrom(Utilities.stringArraytoByteArray(s)));
                argumentTypes.add(ApiaryWorker.stringArrayType);
            } else if (o instanceof int[]) {
                int[] i = (int[]) o;
                byteArguments.add(ByteString.copyFrom(Utilities.intArrayToByteArray(i)));
                argumentTypes.add(ApiaryWorker.intArrayType);
            } else {
                logger.info("Unrecognized type {}: {}", o.getClass().getName(), o);
            }
        }
        long sendTime = System.nanoTime();
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder()
                .setName(name)
                .addAllArguments(byteArguments)
                .addAllArgumentTypes(argumentTypes)
                .setCallerId(callerID)
                .setFunctionId(functionID)
                .setService(service)
                .setExecutionId(execID)
                .setSenderTimestampNano(sendTime)
                .build();
        return req.toByteArray();
    }

    public FunctionOutput executeFunction(String address, String name, String service, long execID, Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        byte[] reqBytes = serializeExecuteRequest(name, service, execID, 0l, 0, arguments);
        socket.send(reqBytes, 0);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        Object output = null;
        if (rep.getReplyType() == ApiaryWorker.stringType) {
            output = rep.getReplyString();
        } else if (rep.getReplyType() == ApiaryWorker.intType) {
            output = rep.getReplyInt();
        } else if (rep.getReplyType() == ApiaryWorker.stringArrayType) {
            output = Utilities.byteArrayToStringArray(rep.getReplyArray().toByteArray());
        } else if (rep.getReplyType() == ApiaryWorker.intArrayType) {
            output = Utilities.byteArrayToIntArray(rep.getReplyArray().toByteArray());
        }
        return new FunctionOutput(output, null);
    }

}
