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
}
