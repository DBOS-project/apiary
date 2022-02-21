package org.dbos.apiary.worker;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// Note: ZMQ.Socket is not thread-safe, so this class is not thread-safe either.
public class ApiaryWorkerClient {

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

    public void shutdown() {
        for (ZMQ.Socket socket: sockets.values()) {
            socket.setLinger(0);
            socket.close();
        }
    }

    public String executeFunction(String name, List<String> arguments, String address) {
        ZMQ.Socket socket = getSocket(address);
        byte[] reqBytes = name.getBytes(StandardCharsets.UTF_8);
        socket.send(reqBytes, 0);
        byte[] replyBytes = socket.recv(0);
        return new String(replyBytes);
    }
}
