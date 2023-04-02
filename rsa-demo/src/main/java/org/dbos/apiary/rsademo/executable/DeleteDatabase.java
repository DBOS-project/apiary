package org.dbos.apiary.rsademo.executable;

import org.dbos.apiary.client.ApiaryWorkerClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeleteDatabase {

    public static void deleteDatabase() throws IOException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");

        List<String> names = new ArrayList<>();
        String namesFile = "src/main/resources/names.txt";
        String line;
        BufferedReader namesReader = new BufferedReader(new FileReader(namesFile));
        while((line = namesReader.readLine()) != null) {
            names.add(line);
        }
        namesReader.close();

        for (String name: names) {
            client.executeFunction("NectarDeletePosts", name);
        }
    }
}
