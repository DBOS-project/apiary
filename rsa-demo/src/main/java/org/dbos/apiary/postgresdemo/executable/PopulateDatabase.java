package org.dbos.apiary.postgresdemo.executable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PopulateDatabase {

    private static final Logger logger = LoggerFactory.getLogger(PopulateDatabase.class);

    private static final String register = "INSERT INTO WebsiteLogins(Username, Password) VALUES (?, ?);";
    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static void populateDatabase(Connection c, int numPeople) throws IOException, SQLException {

        List<String> names = new ArrayList<>();
        List<String> posts = new ArrayList<>();

        String namesFile = "src/main/resources/names.txt";
        String postsFile = "src/main/resources/posts.txt";

        String line;

        BufferedReader namesReader = new BufferedReader(new FileReader(namesFile));
        while((line = namesReader.readLine()) != null) {
            names.add(line);
        }
        namesReader.close();

        BufferedReader postsReader = new BufferedReader(new FileReader(postsFile));
        while((line = postsReader.readLine()) != null) {
            posts.add(line);
        }
        postsReader.close();

        int num = 0;
        while (names.size() < numPeople) {
            names.add("name" + num++);
        }

        assert(posts.size() <= names.size());

        PreparedStatement registerStatement = c.prepareStatement(register);
        for (String name: names) {
            registerStatement.setString(1, name);
            registerStatement.setString(2, name);
            registerStatement.addBatch();
        }
        registerStatement.executeBatch();
        registerStatement.close();

        PreparedStatement postStatement = c.prepareStatement(addPost);
        for (String name: names) {
            for (int postNum = 0; postNum < posts.size(); postNum++) {
                postStatement.setString(1, names.get(postNum));
                postStatement.setString(2, name);
                postStatement.setString(3, posts.get(postNum));
                postStatement.addBatch();
            }
        }
        postStatement.executeBatch();
        postStatement.close();
    }
}
