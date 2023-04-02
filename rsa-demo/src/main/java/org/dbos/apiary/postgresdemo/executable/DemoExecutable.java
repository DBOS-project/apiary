package org.dbos.apiary.postgresdemo.executable;

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

    public static void main(String[] args) throws ParseException, IOException, SQLException {
        Options options = new Options();
        options.addOption("s", true, "Script to run");
        options.addOption("numUsers", true, "Number of Users");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        assert (cmd.hasOption("s"));
        String script = cmd.getOptionValue("s");

        logger.info("Running script: {}", script);

        // Create a connection to the backend database.
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setServerNames(new String[] {"localhost"});
        pg.setPortNumbers(new int[] {ApiaryConfig.postgresPort});
        pg.setDatabaseName(ApiaryConfig.dbosDBName);
        pg.setUser("postgres");
        pg.setPassword("dbos");
        pg.setSsl(false);
        Connection pgConn = pg.getConnection();

        if (script.equals("populateDatabase")) {
            assert(cmd.hasOption("numUsers"));
            int numUsers = Integer.parseInt(cmd.getOptionValue("numUsers"));
            PopulateDatabase.populateDatabase(pgConn, numUsers);
        } else if (script.equals("deleteDatabase")) {
            DeleteDatabase.deleteDatabase();
        } else {
            logger.info("Unknown Script: {}", script);
        }
    }
}
