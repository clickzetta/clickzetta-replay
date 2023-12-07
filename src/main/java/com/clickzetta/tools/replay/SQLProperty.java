package com.clickzetta.tools.replay;

import lombok.Getter;
import lombok.Setter;

public class SQLProperty {
    @Getter
    @Setter
    private String sql;

    @Getter
    @Setter
    private Long originStartTime;

    @Getter
    @Setter
    private Long originElapsedTime = -1L;

    @Getter
    @Setter
    private String viewId;

    @Getter
    @Setter
    private String sqlId;

    @Getter
    @Setter
    private long startTime;

    @Getter
    @Setter
    private long elapsedTime;

    @Getter
    @Setter
    private long originResultCount = -1L;

    @Getter
    @Setter
    private long resultCount = -1;

    @Getter
    @Setter
    private String jobId;

    public String getUniqueKey() {
        return viewId + "_" + sqlId;
    }

    @Override
    public String toString() {
        String res = viewId + "," + sqlId + "," + jobId + "," + startTime + "," + elapsedTime + "," + originStartTime
                        + "," + originElapsedTime + ",";
        if (resultCount == -1) {
            res += "FAILED";
        } else {
            res += resultCount;
        }
        return res;
    }
}
