#FlinkSQL  
##GROUP-BY COUNT  
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/dynamic_tables.html#continuous-queries)  
source: clicks-> user,url  
sql:select user, count(url) as cnt from clicks group by user  

##hourly tumbling window   小时的翻滚窗口  
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/dynamic_tables.html#continuous-queries)  
source: clicks-> user,cTime,url  
sql:select user, TUMBLE_END(cTime, INTERVAL '1' HOURS) as endT, count(url) as cnt, from clicks group by user TUMBLE(cTime, INTERVAL '1' HOURS)  
10分钟的翻滚时间窗口  
sql:select user, TUMBLE_END(cTime, INTERVAL '10' MINUTES) as endT, count(url) as cnt, from clicks group by user TUMBLE(cTime, INTERVAL '10' MINUTES)  

### Group Windows
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sql/queries.html#group-windows)  
#### tumbling time window 翻滚时间窗口
TUMBLE(time_attr, interval)	  
#### sliding time window 滑动时间窗口
HOP(time_attr, interval, interval)   
#### session time window Session时间窗口
SESSION(time_attr, interval)  

## 时间属性
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/time_attributes.html#processing-time)  
env设置时间属性：  
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default  
### Processing time  如何定义
#### Defining in create table DDL  
~~~sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time AS PROCTIME() -- declare an additional field as a processing time attribute
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
~~~  
  
#### During DataStream-to-Table Conversion  
~~~
DataStream<Tuple2<String, String>> stream = ...;

// declare an additional logical field as a processing time attribute
Table table = tEnv.fromDataStream(stream, "user_name, data, user_action_time.proctime");

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
~~~  
  
#### Using a TableSource
~~~
// define a table source with a processing attribute
public class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name" , "data"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// create stream
		DataStream<Row> stream = ...;
		return stream;
	}

	@Override
	public String getProctimeAttribute() {
		// field with this name will be appended as a third field
		return "user_action_time";
	}
}

// register table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
~~~  
  
### Event time  如何定义  
#### Defining in create table DDL  
~~~
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
~~~  
  
#### During DataStream-to-Table Conversion  
~~~
// Option 1:

// extract timestamp and assign watermarks based on knowledge of the stream
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// declare an additional logical field as an event time attribute
Table table = tEnv.fromDataStream(stream, "user_name, data, user_action_time.rowtime");


// Option 2:

// extract timestamp from first field, and assign watermarks based on knowledge of the stream
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// the first field has been used for timestamp extraction, and is no longer necessary
// replace first field with a logical event time attribute
Table table = tEnv.fromDataStream(stream, "user_action_time.rowtime, user_name, data");

// Usage:

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
~~~   
   
#### Using a TableSource
~~~
// define a table source with a rowtime attribute
public class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name", "data", "user_action_time"};
		TypeInformation[] types =
		    new TypeInformation[] {Types.STRING(), Types.STRING(), Types.LONG()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// create stream
		// ...
		// assign watermarks based on the "user_action_time" attribute
		DataStream<Row> stream = inputStream.assignTimestampsAndWatermarks(...);
		return stream;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		// Mark the "user_action_time" attribute as event-time attribute.
		// We create one attribute descriptor of "user_action_time".
		RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
			"user_action_time",
			new ExistingField("user_action_time"),
			new AscendingTimestamps());
		List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
		return listRowtimeAttrDescr;
	}
}

// register the table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
~~~   
   
### Ingestion time  如何定义

## Selecting Group Window Start and End Timestamps  
### 窗口下限时间  
TUMBLE_START(time_attr, interval)   
HOP_START(time_attr, interval, interval)  
SESSION_START(time_attr, interval)  
### 窗口上限时间  
TUMBLE_END(time_attr, interval)  
HOP_END(time_attr, interval, interval)  
SESSION_END(time_attr, interval)  
### 窗口 Event 时间  
TUMBLE_ROWTIME(time_attr, interval)  
HOP_ROWTIME(time_attr, interval, interval)  
SESSION_ROWTIME(time_attr, interval)  
### 窗口 Process 时间  
TUMBLE_PROCTIME(time_attr, interval)  
HOP_PROCTIME(time_attr, interval, interval)  
SESSION_PROCTIME(time_attr, interval)  
like：   
~~~
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);
// register the DataStream as table "Orders"
//proctime.proctime, rowtime.rowtime
tableEnv.createTemporaryView("Orders", ds, "user, product, amount, proctime.proctime, rowtime.rowtime");

// compute SUM(amount) per day (in event-time)
Table result1 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,  " +
  "  SUM(amount) FROM Orders " +
  "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user");

// compute SUM(amount) per day (in processing-time)
Table result2 = tableEnv.sqlQuery(
  "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");

// compute every hour the SUM(amount) of the last 24 hours in event-time
Table result3 = tableEnv.sqlQuery(
  "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");

// compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
Table result4 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
  "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
  "  SUM(amount) " +
  "FROM Orders " +
  "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");
~~~  
  
  
# Join
## Regular Joins 常规连接
this operation has an important implication: it requires to keep both sides of the join input   
in Flink’s state forever. Thus, the resource usage will grow indefinitely as well, if one or both   
input tables are continuously growing.  
## Time-windowed Joins  
~~~
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
~~~  
只支持有时间属性的表 join  
## Join with a Temporal Table Function  
即 你可以使用 Temporal Table 或者 使用 Temporal Table Function,但是需要注意维表的信息更新的话，Temporal Table 不会及时更新的    
在使用时态表(Temporal Table)时，要注意以下问题:  
1. Temporal Table可提供历史某个时间点上的数据。  
2. Temporal Table根据时间来跟踪版本。  
3. Temporal Table需要提供时间属性和主键。  
4. Temporal Table一般和关键词LATERAL TABLE结合使用。  
5. Temporal Table在基于ProcessingTime时间属性处理时，每个主键只保存最新版本的数据。  
6. Temporal Table在基于EventTime时间属性处理时，每个主键保存从上个Watermark到当前系统时间的所有版本。  
7. 左侧Append-Only表Join右侧Temporal Table，本质上还是左表驱动Join，即从左表拿到Key，根据Key和时间(可能是历史时间)去右侧Temporal Table表中查询。  
8. Temporal Table Join目前只支持Inner Join。  
9. Temporal Table Join时，右侧Temporal Table表返回最新一个版本的数据。举个栗子，左侧事件时间如是2016-01-01 00:00:01秒，Join时，只会从右侧Temporal Table中选取<=2016-01-01 00:00:01的最新版本的数据。  
TODO: 在EventTime下，左表WaterMark、右表Temporal Table WaterMark,以及乱序时间之间，关系到底是啥，还需深究。  

### temporal tables 时态表  
时态表函数，不能可以通过sql直接查询某个时间点下的 时态表的数据,只能在join过程中使用这个时态表函数的数据    
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/temporal_tables.html#temporal-table)  
### Temporal Table Function 时态表函数  
时态表函数，不能直接查询某个时间点下的 时态表函数的数据，只能在join过程中使用这个时态表函数的数据  
[参考](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/temporal_tables.html#temporal-table-function)  
## Join with a Lookup Function   
这个 Lookup Function 的作用就是 去查 数据源的数据，如果没有配置 缓存的话，就是 实时查询 数据源的  
