package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
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


// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerClient.class);

    private final ZContext zContext;

    public ApiaryWorkerClient(ZContext zContext) {
        this.zContext = zContext;
    }

    private final Map<String, ZMQ.Socket> sockets = new HashMap<>();

    public ZMQ.Socket getSocket(String address) {
        if (sockets.containsKey(address)) {
            return sockets.get(address);
        } else {
            ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
            socket.connect("tcp://" + address + ":" + ApiaryConfig.workerPort);
            sockets.put(address, socket);
            return socket;
        }
    }

    // Send the function execution request to a socket. Do not wait for response.
    public static void sendExecuteRequest(ZMQ.Socket socket, String name, long callerID, int taskID, Object... arguments) {
        List<ByteString> byteArguments = new ArrayList<>();
        List<Integer> argumentTypes = new ArrayList<>();
        for (Object o: arguments) {
            if (o instanceof String) {
                String s = (String) o;
                byteArguments.add(ByteString.copyFrom(s.getBytes(StandardCharsets.UTF_8)));
                argumentTypes.add(ApiaryWorker.stringType);
            } else {
                assert(o instanceof String[]);
                String[] s = (String[]) o;
                byteArguments.add(ByteString.copyFrom(Utilities.stringArraytoByteArray(s)));
                argumentTypes.add(ApiaryWorker.stringArrayType);
            }
        }
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder()
                .setName(name)
                .addAllArguments(byteArguments)
                .addAllArgumentTypes(argumentTypes)
                .setCallerId(callerID)
                .setTaskId(taskID)
                .build();
        socket.send(req.toByteArray(), 0);
    }

    // Send the function execution response to a socket.
    public static void sendExecuteReply(ZMQ.Socket socket, long callerID, int taskId, String output, ZFrame replyAddr) {
        ExecuteFunctionReply rep = ExecuteFunctionReply.newBuilder().setReply(output)
                .setCallerId(callerID)
                .setTaskId(taskId).build();
        replyAddr.send(socket, ZFrame.REUSE + ZFrame.MORE);
        ZFrame replyContent = new ZFrame(rep.toByteArray());
        replyContent.send(socket, 0);
    }

    // Synchronous blocking invocation, supposed to be used by client/loadgen.
    public String executeFunction(String address, String name, Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        sendExecuteRequest(socket, name, 0L, 0, arguments);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        return rep.getReply();
    }
}
