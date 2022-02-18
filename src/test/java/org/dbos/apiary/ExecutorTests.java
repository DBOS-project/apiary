package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.Executor;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.catalog.ThreadPool;
import org.voltdb.client.ProcCallException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExecutorTests {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testSerialization() {
        logger.info("testSerialization");
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
    }

    @Test
    public void testAdditionExec() throws Exception {
        logger.info("testAdditionExec");
        ApiaryConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "AdditionFunction", 0, "1", "2", new String[]{"matei", "zaharia"});
        assertEquals("3mateizaharia", res);
    }

    @Test
    public void testFibExec() throws Exception {
        logger.info("testFibExec");
        ApiaryConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        String res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "1");
        assertEquals("1", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "3");
        assertEquals("2", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "4");
        assertEquals("3", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "10");
        assertEquals("55", res);
        res = Executor.executeFunction(ctxt, "FibonacciFunction", defaultPkey, "30");
        assertEquals("832040", res);
    }

    @Test
    public void testSockets() throws IOException {
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
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        out.println(inputLine + "!!!");
                        if (inputLine.equals("bye")) {
                            break;
                        }
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
                int numTrials = 1000;
                Socket client = new Socket("localhost", 8001);
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                for (int i = 0; i < numTrials; i++) {
                    int number = count.getAndIncrement();
                    long t0 = System.nanoTime();
                    out.println(number);
                    String fromServer = in.readLine();
                    long elapsed = System.nanoTime() - t0;
                    assertEquals(number + "!!!", fromServer);
                }
                out.println("bye");
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
