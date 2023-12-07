package com.clickzetta.tools.replay;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLConverter {
    private static final Map<String, String> sqlPatterns = new LinkedHashMap<>();

    static {
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd HH24:MI:SS'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd HH:mm:ss')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd hh:MI:ss'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd hh:mm:ss')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd HH24:MI'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd HH:mm')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd HH24'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd HH')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd hh'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd hh')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd hh:MI'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd hh:mm')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM-dd'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM-dd')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-MM'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-\\\"Q\\\"Q'\\s*\\)", "concat(date_format(convert_timezone(\\2, \\1 div 1000),'yyyy'), '-', date_format(convert_timezone(\\2, \\1 div 1000),'QQQ'))");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy-IW'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy-MM')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'IYYY-IW'\\s*\\)", "concat(yearofweek(convert_timezone(\\2, \\1 div 1000)),'-',weekofyear(convert_timezone(\\2, \\1 div 1000)))");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'IYYY'\\s*\\)", "yearofweek(convert_timezone(\\2, \\1 div 1000))");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'IW'\\s*\\)", "weekofyear(convert_timezone(\\2, \\1 div 1000))");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'yyyy'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'yyyy')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'MM'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'MM')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'dd'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'dd')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'hh'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'hh')");
        sqlPatterns.put("to_char\\s*\\(\\s*to_timestamp\\s*\\(\\s*([^)]+)\\s*/\\s*1000\\s*\\)\\s*AT\\s*TIME\\s*ZONE\\s*([^)]+),\\s*'\\\"Q\\\"Q'\\s*\\)", "date_format(convert_timezone(\\2, \\1 div 1000),'QQQ')");
        sqlPatterns.put("\\bdate_part\\b\\s*\\(\\s*'epoch'\\s*,\\s*now\\s*\\(\\s*\\)\\s*\\)", "to_unix_timestamp(now())");
        sqlPatterns.put("SAVEPOINT\\s*([^\\s]+)", "");
    }

    public static String convert(String sql) {
        String convertedSql = sql;
        // 遍历所有的正则表达式
        for (Map.Entry<String, String> entry : sqlPatterns.entrySet()) {
            Pattern pattern = Pattern.compile(entry.getKey());
            Matcher matcher = pattern.matcher(convertedSql);
            convertedSql = matcher.replaceAll(entry.getValue());
        }
        return convertedSql;
    }
}
