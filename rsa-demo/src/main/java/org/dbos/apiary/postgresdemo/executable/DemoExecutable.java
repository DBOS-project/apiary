package org.dbos.apiary.postgresdemo.executable;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class DemoExecutable {

    private static final Logger logger = LoggerFactory.getLogger(DemoExecutable.class);

    public static void main(String[] args) throws ParseException, SQLException {
        Options options = new Options();
        options.addOption("s", true, "Script to run");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        assert (cmd.hasOption("s"));
        String script = cmd.getOptionValue("s");

        logger.info("Running script: {}", script);
    }
}
