package com.clickzetta.tools.replay;

import cz.shade.org.apache.commons.cli.*;

import java.io.*;
import java.util.Properties;

public class ReplaySQL {
    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseCmdParameters(args);
        Config config = loadConfigFile(cmd);

        SQLExecutor executor = new SQLExecutor(config);
        SQLParser parser = new SQLParser(cmd.getOptionValue("file"));

        SQLProperty sqlProperty;
        while ((sqlProperty = parser.getSQL()) !=null) {
            executor.execute(sqlProperty);
        }

        parser.close();
        executor.close();
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
                        .hasArg(true).required(true)
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
        config.setThreadCount(Integer.parseInt(cmd.getOptionValue("thread", "1")));
        config.setReplayRate(Integer.parseInt(cmd.getOptionValue("rate", "1")));
        config.setOutputFile(cmd.getOptionValue("output", "output.txt"));
        config.setWithoutDelay(cmd.hasOption("without delay"));
        reader.close();
        return config;
    }
}
