package org.dbos.apiary.benchmarks;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoExecutable {
    private static final Logger logger = LoggerFactory.getLogger(DemoExecutable.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");
        options.addOption("execId", true, "The target execution ID for replay.");
        options.addOption("mode", true, "0-not replay, 2-replay all after the target execID.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String mainHostAddr = "localhost";
        if (cmd.hasOption("mainHostAddr")) {
            mainHostAddr = cmd.getOptionValue("mainHostAddr");
        }

        int replayMode = 0;
        long execId = 0l;
        if (cmd.hasOption("retroMode")) {
            replayMode = Integer.parseInt(cmd.getOptionValue("mode"));
            if (replayMode > 0) {
                execId = Long.parseLong(cmd.getOptionValue("execId"));
            }
        }
        RetroDemo.benchmark(mainHostAddr, replayMode, execId);
    }
}
