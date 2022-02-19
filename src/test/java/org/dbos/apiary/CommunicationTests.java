package org.dbos.apiary;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommunicationTests {

    private static final Logger logger = LoggerFactory.getLogger(CommunicationTests.class);

    @Test
    public void testSockets() {
        logger.info("testSockets");
        ExecutorService threadPool = Executors.newFixedThreadPool(256);
        AtomicBoolean listening = new AtomicBoolean(true);
        class ServerThread implements Runnable {

            final Socket socket;

            ServerThread(Socket socket) {
                this.socket = socket;
            }

            @Override
            public void run() {
                try {
                    int inputByte;
                    while ((inputByte = socket.getInputStream().read()) != 101) {
                        socket.getOutputStream().write(inputByte);
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Runnable serverRunnable = () -> {
            try {
                ServerSocket serverSocket = new ServerSocket(8001);
                while (listening.get()) {
                    threadPool.submit(new ServerThread(serverSocket.accept()));
                }
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        threadPool.submit(serverRunnable);
        int numClients = 10;
        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger finished = new AtomicInteger(numClients);
        Runnable clientRunnable = () -> {
            try {
                int numTrials = 10;
                Socket client = new Socket("localhost", 8001);
                for (int i = 0; i < numTrials; i++) {
                    int number = count.getAndIncrement() % 100;
                    long t0 = System.nanoTime();
                    client.getOutputStream().write(number);
                    int fromServer = client.getInputStream().read();
                    long elapsed = System.nanoTime() - t0;
                    assertEquals(number, fromServer);
                }
                client.getOutputStream().write(101);
                client.close();
                finished.decrementAndGet();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        for (int i = 0; i < numClients; i++) {
            threadPool.submit(clientRunnable);
        }
        while (finished.get() > 0) { }
        listening.set(false);
        threadPool.shutdown();
    }

    @Test
    public void testZMQ() throws InterruptedException {
        logger.info("testZMQ");

        Runnable serverRunnable = () -> {
            try (ZContext context = new ZContext()) {
                ZMQ.Socket frontend = context.createSocket(SocketType.ROUTER);
                frontend.bind("tcp://*:" + 8001);
                ZMQ.Socket backend = context.createSocket(SocketType.DEALER);
                backend.bind("inproc://backend");

                Runnable serverThreadRunnable = () -> {
                    ZMQ.Socket worker = context.createSocket(SocketType.REP);
                    worker.connect("inproc://backend");
                    while (!Thread.currentThread().isInterrupted()) {
                        byte[] b = worker.recv(0);
                        String input = new String(b, StandardCharsets.UTF_8);
                        String output = input + "!!!";
                        worker.send(output.getBytes(StandardCharsets.UTF_8));
                    }
                };
                for (int i = 0; i < 10; i++) {
                    new Thread(serverThreadRunnable).start();
                }
                ZMQ.proxy(frontend, backend, null);
            }
        };

        Thread server = new Thread(serverRunnable);
        server.start();

        ExecutorService threadPool = Executors.newFixedThreadPool(256);
        Runnable clientRunnable = () -> {
            try (ZContext context = new ZContext()) {
                ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.connect("tcp://*:" + 8001);
                for (int i = 0; i < 100; i++) {
                    String input = String.valueOf(i);
                    socket.send(input.getBytes(StandardCharsets.UTF_8));
                    byte[] b = socket.recv(0);
                    String output = new String(b, StandardCharsets.UTF_8);
                    assertEquals(i + "!!!", output);
                }
            }
        };
        for (int i = 0; i < 10 ; i++) {
            threadPool.submit(clientRunnable);
        }
        threadPool.shutdown();

        server.join();
    }
}
