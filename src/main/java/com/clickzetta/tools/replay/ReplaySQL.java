package com.clickzetta.tools.replay;

import org.apache.commons.cli.*;

import java.io.*;
import java.util.Properties;

public class ReplaySQL {
    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseCmdParameters(args);
        Config config = loadConfigFile(cmd);

        SQLExecutor executor = SQLExecutorFactory.getExecutor(config);

        executor.execute();
    }

    private static CommandLine parseCmdParameters(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder()
                        .option("h").longOpt("help")
                        .desc("print help")
                        .hasArg(false).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("f").longOpt("file")
                        .desc("input data file")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("c").longOpt("config")
                        .desc("connection properties")
                        .hasArg(true).required(true)
                        .build())
                .addOption(Option.builder()
                        .option("t").longOpt("thread")
                        .desc("connection pool size")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("r").longOpt("rate")
                        .desc("replay rate")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("w").longOpt("without delay")
                        .desc("without delay")
                        .hasArg(false).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("o").longOpt("output")
                        .desc("output file")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("ot").longOpt("output timeout")
                        .desc("sql output timeout")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("p").longOpt("port")
                        .desc("replay server port")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("d").longOpt("dynamic mode")
                        .desc("dynamic mode")
                        .hasArg(false).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("s").longOpt("sleep interval")
                        .desc("sleep interval ms")
                        .hasArg(true).required(false)
                        .build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("clickzetta load table", options);
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("clickzetta load table", options);
            System.exit(0);
        }
        return cmd;
    }

    private static Config loadConfigFile(CommandLine cmd) throws IOException {
        Config config = new Config();
        FileReader reader = new FileReader(cmd.getOptionValue("config"));
        Properties prop = new Properties();
        prop.load(reader);
        config.setJdbcUrl(prop.getProperty("jdbcUrl"));
        config.setUsername(prop.getProperty("username"));
        config.setPassword(prop.getProperty("password"));
        config.setDriverClass(prop.getProperty("driver"));
        config.setInputFile(cmd.getOptionValue("file"));
        config.setThreadCount(Integer.parseInt(cmd.getOptionValue("thread", "1")));
        config.setReplayRate(Integer.parseInt(cmd.getOptionValue("rate", "1")));
        config.setOutputFile(cmd.getOptionValue("output", "output.txt"));
        config.setWithoutDelay(cmd.hasOption("without delay"));
        config.setOutputTimeout(Integer.parseInt(cmd.getOptionValue("output timeout", "30000")));
        config.setServerPort(Integer.parseInt(cmd.getOptionValue("port", "28082")));
        config.setDynamicMode(cmd.hasOption("dynamic mode"));
        config.setSleepInterval(Integer.parseInt(cmd.getOptionValue("sleep interval", "100")));
        reader.close();
        return config;
    }
}
