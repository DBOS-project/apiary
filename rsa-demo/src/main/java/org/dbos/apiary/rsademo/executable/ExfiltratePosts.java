package org.dbos.apiary.rsademo.executable;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ExfiltratePosts {

    private static final Logger logger = LoggerFactory.getLogger(ExfiltratePosts.class);

    private static final String associateName = "John Q Hacker";

    public static void exfiltratePosts(String username) throws IOException, InterruptedException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");

        System.out.println("Retrieving messages from: " + username);
        String[] posts = client.executeFunction("NectarGetPosts", username).getStringArray();
        if (posts != null) {
            System.out.println("Retrieve Successful");
        } else {
            System.out.println("Error: Retrieve failed.");
            return;
        }
        Thread.sleep(500);
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
