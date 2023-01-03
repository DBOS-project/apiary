package org.dbos.apiary.benchmarks;

import org.apache.commons.cli.*;

public class DemoExecutable {
    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");
        options.addOption("execId", true, "The target execution ID for replay.");
        options.addOption("mode", true, "0-not replay, 1-replay the original trace, 2-replay with the modified code.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String mainHostAddr = "localhost";
        if (cmd.hasOption("mainHostAddr")) {
            mainHostAddr = cmd.getOptionValue("mainHostAddr");
        }

        int replayMode = 0;
        long execId = 0l;
        if (cmd.hasOption("mode")) {
            replayMode = Integer.parseInt(cmd.getOptionValue("mode"));
            if (replayMode > 0) {
                execId = Long.parseLong(cmd.getOptionValue("execId"));
            }
        }
        RetroDemo.benchmark(mainHostAddr, replayMode, execId);
    }
}
