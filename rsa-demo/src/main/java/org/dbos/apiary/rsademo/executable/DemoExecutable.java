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
        options.addOption("startId", true, "Start request ID for replay");
        options.addOption("endId", true, "End request ID for replay");
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
        } else if (script.equalsIgnoreCase("replay")) {
            long startExecId = Long.parseLong(cmd.getOptionValue("startId"));
            long endExecId = cmd.hasOption("endId") ? Long.parseLong(cmd.getOptionValue("endId")) : Long.MAX_VALUE;
            logger.info("Replay requests between [{}, {})", startExecId, endExecId);
            Replay.replay(startExecId, endExecId);
        } else if (script.equalsIgnoreCase("resetTables")) {
            ResetDatabase.resetDatabase("localhost");
        } else {
            logger.info("Unknown Script: {}", script);
        }
    }
}
