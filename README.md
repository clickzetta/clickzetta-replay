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
-w: 重放sql间不执行delay操作
-p: http server监听端口
-ot: sql结果输出超时时间（默认30s）
-h: 帮助
```
本工具支持两种模式，一种是离线工具模式，一种是http server模式。离线工具模式下，需要指定`-f`参数，指定待重放sql文件，
如不指定`-f`参数, 则会进入http server模式，监听28082端口，接收post请求，请求参数为待重放sql

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
> http请求重放的sql输出结果和上述文件格式一致，只是view_id字段为pg sql发起的途径，id为pg sql的序列id，这二者均不保证唯一性