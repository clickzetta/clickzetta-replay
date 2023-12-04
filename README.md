# ClickZetta Replay
clickzetta的sql重放工具

## 编译
```shell
mvn clean package
```

## 使用
```shell
java -jar clickzetta-replay-1.0-SNAPSHOT.jar -t <线程数> -r <重放速率> -f <待重放sql文件> -c <配置文件> -o <输出文件>

# 其他参数
-w : 重放sql间不执行delay操作
-h: 帮助
```

## 配置文件
```yaml
# 数据库连接信息
jdbcUrl=jdbc:example://example.api.clickzetta.com/workspace?virtualCluster=example&schema=example
username=example
password=example
driver=com.clickzetta.client.jdbc.ClickZettaDriver
```

## 待重放SQL文件
```sql
view_id:1234567890 id:1234567 start_time:1701216058985 elapsed_time:241 sql: select 1;
view_id:1234567891 id:1234568 start_time:1701216061630 elapsed_time:231 sql: select 2;
view_id:1234567892 id:1234569 start_time:1701216063554 elapsed_time:214 sql: select 3;
```

## 输出文件
```
# 重放结果
# view_id, id, job_id, new_job_start_time, new_job_elapsed_time, origin_job_start_time, origin_job_elapsed_time, sql_result_count
3023880575222212,3054803401671177,2023120410152119261v2hkb7xxxxx,1701656121200,27584,1701216058985,241,0
```