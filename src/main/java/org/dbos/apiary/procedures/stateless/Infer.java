package org.dbos.apiary.procedures.stateless;

import org.dbos.apiary.stateless.StatelessFunction;

import java.net.*;
import java.io.*;

public class Infer extends StatelessFunction {

    public Integer[] runFunction(String inputString) throws UnknownHostException, IOException {
        TCPClient client = new TCPClient();
        client.startConnection("localhost", 6666);
        
        String response = client.sendMessage(inputString);
        String[] classificationStrings = response.split("&");
        Integer[] classifications = new Integer[classificationStrings.length];
        for (int i = 0; i < classificationStrings.length; i++) {
            classifications[i] = Integer.parseInt(classificationStrings[i]);
        }
        
        return classifications;
    }
}
