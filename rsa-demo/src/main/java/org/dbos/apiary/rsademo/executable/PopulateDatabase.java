package org.dbos.apiary.rsademo.executable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PopulateDatabase {

    private static final Logger logger = LoggerFactory.getLogger(PopulateDatabase.class);

    private static final String register = "INSERT INTO WebsiteLogins(Username, Password) VALUES (?, ?);";
    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static void populateDatabase(Connection c) throws IOException, SQLException {

        List<String> firstNames = new ArrayList<>();
        List<String> lastNames = new ArrayList<>();
        List<String> posts = new ArrayList<>();

        String firstNamesFile = "src/main/resources/firstnames.txt";
        String lastNamesFile = "src/main/resources/lastnames.txt";
        String postsFile = "src/main/resources/posts.txt";

        String line;

        BufferedReader namesReader = new BufferedReader(new FileReader(firstNamesFile));
        while((line = namesReader.readLine()) != null) {
            firstNames.add(line);
        }
        namesReader.close();
        namesReader = new BufferedReader(new FileReader(lastNamesFile));
        while((line = namesReader.readLine()) != null) {
            lastNames.add(line);
        }

        BufferedReader postsReader = new BufferedReader(new FileReader(postsFile));
        while((line = postsReader.readLine()) != null) {
            posts.add(line);
        }
        postsReader.close();

        List<String> names = new ArrayList<>();
        for (String firstName: firstNames) {
            for (String lastName: lastNames) {
                names.add(firstName + " " + lastName);
            }
        }

        PreparedStatement registerStatement = c.prepareStatement(register);
        for (String name: names) {
            registerStatement.setString(1, name);
            registerStatement.setString(2, "test");
            registerStatement.addBatch();
        }
        registerStatement.executeBatch();
        registerStatement.close();

        PreparedStatement postStatement = c.prepareStatement(addPost);
        for (String name: names) {
            if (name.equals("John Q Hacker")) {
              continue;
            }
            for (String post : posts) {
                postStatement.setString(1, names.get(ThreadLocalRandom.current().nextInt(names.size())));
                postStatement.setString(2, name);
                postStatement.setString(3, post);
                postStatement.addBatch();
            }
        }
        postStatement.executeBatch();
        postStatement.close();
    }
}
