package org.dbos.apiary.benchmarks;

import org.apache.commons.cli.*;

public class DemoExecutable {
    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("mainHostAddr", true, "Address of the main host to connect to.");
        options.addOption("execId", true, "The target execution ID for replay.");
        options.addOption("mode", true, "Options are: [exec, replay, retro]");
        options.addOption("numReq", true, "Number of requests to be executed, default 10.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String mainHostAddr = "localhost";
        if (cmd.hasOption("mainHostAddr")) {
            mainHostAddr = cmd.getOptionValue("mainHostAddr");
        }

        int replayMode = 0;
        long execId = 0l;
        int numExec = 10;
        if (cmd.hasOption("numReq")) {
            numExec = Integer.parseInt(cmd.getOptionValue("numReq"));
        }
        if (cmd.hasOption("mode")) {
            String modeStr = cmd.getOptionValue("mode");
            if (modeStr.equals("exec")) {
                replayMode = RetroDemo.DemoMode.NOT_REPLAY.getValue();
            } else if (modeStr.equals("replay")) {
                replayMode = RetroDemo.DemoMode.REPLAY.getValue();
            } else if (modeStr.equals("retro")) {
                replayMode = RetroDemo.DemoMode.RETRO.getValue();
            }
            if (replayMode > 0) {
                execId = Long.parseLong(cmd.getOptionValue("execId"));
            }
        }
        RetroDemo.benchmark(mainHostAddr, replayMode, execId, numExec);
    }
}
