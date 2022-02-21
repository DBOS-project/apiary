package org.dbos.apiary.worker;

import org.dbos.apiary.executor.ApiaryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ApiaryWorker {
    private static final Logger logger = LoggerFactory.getLogger(ApiaryWorker.class);

    private static final int numWorkerThreads = 8;

    private final ApiaryConnection c;
    private final int serverPort;
    private ZContext zContext;
    private Thread serverThread;
    private final List<Thread> workerThreads = new ArrayList<>();

    public ApiaryWorker(int serverPort, ApiaryConnection c) {
        this.serverPort = serverPort;
        this.c = c;
        this.zContext = new ZContext();
    }

    private void workerThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket worker = shadowContext.createSocket(SocketType.REP);
        worker.connect("inproc://backend");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] b = worker.recv(0);
                String input = new String(b, StandardCharsets.UTF_8);
                String output = input + "!!!";
                worker.send(output.getBytes(StandardCharsets.UTF_8));
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode() || e.getErrorCode() == ZMQ.Error.EINTR.getCode()) {
                    break;
                } else {
                    e.printStackTrace();
                }
            }
        }
        shadowContext.close();
    }

    private void serverThread() {
        ZContext shadowContext = ZContext.shadow(zContext);
        ZMQ.Socket frontend = shadowContext.createSocket(SocketType.ROUTER);
        frontend.bind("tcp://*:" + serverPort);

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
