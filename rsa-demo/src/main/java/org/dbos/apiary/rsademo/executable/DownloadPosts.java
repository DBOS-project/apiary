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

    public static void downloadPosts(String outputFile) throws IOException {
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", "admin_2");
        List<String> names = getNames();

        CSVWriter csvWriter = new CSVWriter(new BufferedWriter(new FileWriter(outputFile)));
        csvWriter.writeNext(new String[]{"Sender", "Receiver", "PostText"});
        for (String name: names) {
            String[] posts = client.executeFunction("NectarGetPosts", name).getStringArray();
            for (String post: posts) {
                JSONObject obj = (JSONObject) JSONValue.parse(post);
                csvWriter.writeNext(new String[]{(String) obj.get("Sender"), name, (String) obj.get("PostText")});
            }

        }
        csvWriter.close();
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
