package org.dbos.apiary.rsademo.executable;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;
import java.io.IOException;

public class ExfiltratePosts {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private static final Logger logger = LoggerFactory.getLogger(ExfiltratePosts.class);

    private static final String associateName = "John Q Hacker";

    public static void exfiltratePosts(String username) throws IOException, InterruptedException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");

        System.out.println(ANSI_YELLOW + "Retrieving messages from: " + username + ANSI_RESET);
        Thread.sleep(1000);
        String[] posts = client.executeFunction("NectarGetPosts", username).getStringArray();
        if (posts != null) {
            System.out.println(ANSI_GREEN + "Retrieve Successful" + ANSI_RESET);
        } else {
            System.out.println(ANSI_RED + "Error: Account suspended." + ANSI_RESET);
            return;
        }
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        System.out.println(ANSI_YELLOW + "Sending messages to: " + associateName + ANSI_RESET);
        boolean success = true;
        Thread.sleep(1000);
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
            System.out.println(ANSI_GREEN + "Exfiltration successful" + ANSI_RESET);
        } else {
            System.out.println(ANSI_RED + "Error: Account suspended" + ANSI_RESET);
        }
    }

}
