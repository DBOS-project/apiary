package org.dbos.apiary.postgresdemo.executable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class PopulateDatabase {

    private static final Logger logger = LoggerFactory.getLogger(PopulateDatabase.class);

    public static void populateDatabase(Connection c, int numPeople) throws IOException {

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

        logger.info("{}", names);
        logger.info("{}", posts);
    }
}
