package org.dbos.apiary.procedures.stateless;

import org.dbos.apiary.stateless.StatelessFunction;

import java.net.*;
import java.io.*;

public class Infer extends StatelessFunction {

    public String runFunction(String inputString) throws UnknownHostException, IOException {
        TCPClient client = new TCPClient();
        client.startConnection("localhost", 6666);
        String response = client.sendMessage(inputString);
        return response;
    }

    private class TCPClient {
        private Socket clientSocket;
        private PrintWriter out;
        private BufferedReader in;

        public void startConnection(String ip, int port) throws UnknownHostException, IOException {
            clientSocket = new Socket(ip, port);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        }

        public String sendMessage(String msg) throws IOException {
            out.println(msg);
            String resp = in.readLine();
            return resp;
        }

        public void stopConnection() throws IOException {
            in.close();
            out.close();
            clientSocket.close();
        }
    }
}
