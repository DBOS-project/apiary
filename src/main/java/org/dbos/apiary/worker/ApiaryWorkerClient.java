package org.dbos.apiary.worker;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class ApiaryWorkerClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorkerClient.class);

    private final ZContext zContext;
    public static final AtomicInteger callerIDs = new AtomicInteger(0);

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
    public static void sendExecuteRequest(ZMQ.Socket socket, String name, int callerID, int taskID, Object... arguments) {
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

    // Synchronous blocking invocation, supposed to be used by client/loadgen.
    public String executeFunction(String address, String name, Object... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        sendExecuteRequest(socket, name, 0, 0, arguments);
        byte[] replyBytes = recvExecuteReply(socket);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        return rep.getReply();
    }

    // Block receiving reply, for synchronous call.
    private byte[] recvExecuteReply(ZMQ.Socket client) {
        ZMQ.Poller poller = zContext.createPoller(1);
        poller.register(client, ZMQ.Poller.POLLIN);
        byte[] results = null;
        // TODO: add hard timeouts?
        poller.poll(100000); // Timeout set to a large value, 100 sec.
        if (poller.pollin(0)) {
            ZMsg msg = ZMsg.recvMsg(client);
            results = msg.getLast().getData();
            msg.destroy();
        }
        poller.close();
        return results;
    }
}
