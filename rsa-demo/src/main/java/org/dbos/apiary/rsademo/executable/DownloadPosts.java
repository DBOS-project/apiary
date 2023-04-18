package org.dbos.apiary.rsademo.executable;

import com.opencsv.CSVWriter;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DownloadPosts {

    private static final Logger logger = LoggerFactory.getLogger(DownloadPosts.class);

    private static final int numAssociates = 100;

    public static void downloadPosts() throws IOException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");
        List<String> names = getNames();

        for (int i = 0; i < numAssociates; i++) {
            String name = "associate" + i;
            client.executeFunction("NectarRegister", name, name);
        }
        int counter = 0;
        int associateCounter = 0;
        for (String name: names) {
            String[] posts = client.executeFunction("NectarGetPosts", name).getStringArray();
            if (posts == null) {
                System.out.println("\nError: Request failed.");
                break;
            }
            for (String post: posts) {
                JSONObject obj = (JSONObject) JSONValue.parse(post);
                String sender = (String) obj.get("Sender");
                String receiver = "associate" + associateCounter;
                associateCounter = (associateCounter + 1) % numAssociates;
                String postText = (String) obj.get("PostText");
                client.executeFunction("NectarAddPost", sender, receiver, postText);
            }
            System.out.printf("\rDownloaded Posts of %d Users", ++counter);
        }
    }


    private static List<String> getNames() throws IOException {
        String firstNamesFile = "src/main/resources/firstnames.txt";
        String lastNamesFile = "src/main/resources/lastnames.txt";
        String line;

        List<String> firstNames = new ArrayList<>();
        List<String> lastNames = new ArrayList<>();

        BufferedReader namesReader = new BufferedReader(new FileReader(firstNamesFile));
        while((line = namesReader.readLine()) != null) {
            firstNames.add(line);
        }
        namesReader.close();
        namesReader = new BufferedReader(new FileReader(lastNamesFile));
        while((line = namesReader.readLine()) != null) {
            lastNames.add(line);
        }

        List<String> names = new ArrayList<>();
        for (String firstName: firstNames) {
            for (String lastName: lastNames) {
                names.add(firstName + " " + lastName);
            }
        }

        return names;
    }
}
