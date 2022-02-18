package org.dbos.apiary;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
}
