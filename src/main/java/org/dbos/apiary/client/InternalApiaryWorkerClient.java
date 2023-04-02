package org.dbos.apiary.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.worker.ApiaryWorker;
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

/**
 * For internal use only. This class is not thread-safe.
 */
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

    public static byte[] serializeExecuteRequest(String name, String role, long execID, int replayMode,
                                                 long callerID, long functionID, Object... arguments) {
        List<ByteString> byteArguments = new ArrayList<>();
        List<Integer> argumentTypes = new ArrayList<>();
        serializeArguments(byteArguments, argumentTypes, arguments);
        long sendTime = System.nanoTime();
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder()
                .setName(name)
                .addAllArguments(byteArguments)
                .addAllArgumentTypes(argumentTypes)
                .setCallerId(callerID)
                .setFunctionId(functionID)
                .setRole(role)
                .setExecutionId(execID)
                .setSenderTimestampNano(sendTime)
                .setReplayMode(replayMode)
                .build();
        return req.toByteArray();
    }

    private static byte[] serializeReplayRequest(String name, String role, long execID, long endExecId, int replayMode,
                                                 long callerID, long functionID, Object... arguments) {
        List<ByteString> byteArguments = new ArrayList<>();
        List<Integer> argumentTypes = new ArrayList<>();
        serializeArguments(byteArguments, argumentTypes, arguments);
        long sendTime = System.nanoTime();
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder()
                .setName(name)
                .addAllArguments(byteArguments)
                .addAllArgumentTypes(argumentTypes)
                .setCallerId(callerID)
                .setFunctionId(functionID)
                .setRole(role)
                .setExecutionId(execID)
                .setSenderTimestampNano(sendTime)
                .setReplayMode(replayMode)
                .setEndExecId(endExecId)
                .build();
        return req.toByteArray();
    }

    public FunctionOutput executeFunction(String address, String name, String role, long execID, int replayMode,
                                          Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        byte[] reqBytes = serializeExecuteRequest(name, role, execID, replayMode, 0l, 0, arguments);
        socket.send(reqBytes, 0);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        Object output = Utilities.getOutputFromReply(rep);
        return new FunctionOutput(output, null, rep.getErrorMsg());
    }

    public FunctionOutput retroReplay(String address, String name, String role, long startExecId, long endExecId,
                                      int replayMode, Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        byte[] reqBytes = serializeReplayRequest(name, role, startExecId, endExecId, replayMode, 0l, 0, arguments);
        socket.send(reqBytes, 0);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        Object output = Utilities.getOutputFromReply(rep);
        return new FunctionOutput(output, null, rep.getErrorMsg());
    }

    private static void serializeArguments (List<ByteString> byteArguments, List<Integer> argumentTypes, Object[] arguments) {
        if (arguments != null) {
            for (Object o : arguments) {
                if (o instanceof String) {
                    String s = (String) o;
                    byteArguments.add(ByteString.copyFrom(s.getBytes(StandardCharsets.UTF_8)));
                    argumentTypes.add(Utilities.stringType);
                } else if (o instanceof Integer) {
                    Integer i = (Integer) o;
                    byteArguments.add(ByteString.copyFrom(Utilities.toByteArray(i)));
                    argumentTypes.add(Utilities.intType);
                } else if (o instanceof String[]) {
                    String[] s = (String[]) o;
                    byteArguments.add(ByteString.copyFrom(Utilities.stringArraytoByteArray(s)));
                    argumentTypes.add(Utilities.stringArrayType);
                } else if (o instanceof int[]) {
                    int[] i = (int[]) o;
                    byteArguments.add(ByteString.copyFrom(Utilities.intArrayToByteArray(i)));
                    argumentTypes.add(Utilities.intArrayType);
                } else {
                    logger.info("Unrecognized type {}: {}", o.getClass().getName(), o);
                }
            }
        }
    }

}
