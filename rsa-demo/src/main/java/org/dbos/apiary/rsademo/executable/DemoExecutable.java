package org.dbos.apiary.rsademo.executable;

import org.apache.commons.cli.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class DemoExecutable {

    private static final Logger logger = LoggerFactory.getLogger(DemoExecutable.class);

    public static void main(String[] args) throws ParseException, IOException, SQLException, InterruptedException {
        Options options = new Options();
        options.addOption("s", true, "Script to run");
        options.addOption("username", true, "Username");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        assert (cmd.hasOption("s"));
        String script = cmd.getOptionValue("s");

        // Create a connection to the backend database.
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setServerNames(new String[] {"localhost"});
        pg.setPortNumbers(new int[] {ApiaryConfig.postgresPort});
        pg.setDatabaseName(ApiaryConfig.dbosDBName);
        pg.setUser("postgres");
        pg.setPassword("dbos");
        pg.setSsl(false);
        Connection pgConn = pg.getConnection();

        if (script.equalsIgnoreCase("populateDatabase")) {
            PopulateDatabase.populateDatabase(pgConn);
        } else if (script.equalsIgnoreCase("exfiltratePosts")) {
            assert(cmd.hasOption("username"));
            String username = cmd.getOptionValue("username");
            ExfiltratePosts.exfiltratePosts(username);
        } else if (script.equalsIgnoreCase("resetTables")) {
            ResetDatabase.resetDatabase("localhost");
        } else {
            logger.info("Unknown Script: {}", script);
        }
    }
}
