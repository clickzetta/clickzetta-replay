package com.clickzetta.tools.replay;

import lombok.Getter;
import lombok.Setter;

public class Config {

    @Getter
    @Setter
    private String jdbcUrl;
    @Getter
    @Setter
    private String username;
    @Getter
    @Setter
    private String password;
    @Getter
    @Setter
    private String driverClass;
    @Getter
    @Setter
    private int threadCount;
    @Getter
    @Setter
    private int replayRate;
    @Getter
    @Setter
    private String inputFile;
    @Getter
    @Setter
    private String outputFile;
    @Getter
    @Setter
    private boolean withoutDelay;
    @Getter
    @Setter
    private int outputTimeout;
    @Getter
    @Setter
    private int serverPort;
}
