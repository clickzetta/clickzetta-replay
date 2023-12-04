package com.clickzetta.tools.replay;

import lombok.Getter;
import lombok.Setter;

public class SQLProperty {
    @Getter
    @Setter
    private String sql;

    @Getter
    @Setter
    private Long startTime;

    @Getter
    @Setter
    private Long elapsedTime;

    @Getter
    @Setter
    private String viewId;

    @Getter
    @Setter
    private String sqlId;
}
