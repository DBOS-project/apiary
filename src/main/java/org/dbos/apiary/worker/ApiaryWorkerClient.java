package org.dbos.apiary.worker;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.ExecuteFunctionReply;
import org.dbos.apiary.ExecuteFunctionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

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

    private ZMQ.Socket getSocket(String address) {
        if (sockets.containsKey(address)) {
            return sockets.get(address);
        } else {
            ZMQ.Socket socket = zContext.createSocket(SocketType.REQ);
            socket.connect("tcp://" + address);
            sockets.put(address, socket);
            return socket;
        }
    }

    public String executeFunction(String address, String name, String... arguments) throws InvalidProtocolBufferException {
        ZMQ.Socket socket = getSocket(address);
        ExecuteFunctionRequest req = ExecuteFunctionRequest.newBuilder().setName(name).addAllArguments(List.of(arguments)).build();
        socket.send(req.toByteArray(), 0);
        byte[] replyBytes = socket.recv(0);
        ExecuteFunctionReply rep = ExecuteFunctionReply.parseFrom(replyBytes);
        return rep.getReply();
    }
}
