# Flinl10_learn
主要用于学习 flink 10 版本的目的   
[github 地址](https://github.com/JSQF/flink10_learn)  
[码云 地址](https://gitee.com/jsqf/flink10_learn)  
## 包目录介绍
DataSet存放 批代码的包

DataStream存放 流代码的包


## flink parquet
1. 增加 flink-parquet maven 依赖 需要添加 flink-parquet 和 parquet-avro 依赖
2. batch 模式下 没有找到可以写 parquet 文件的方法，DataStream 模式下 可以通过 StreamingFileSink 的 Bulk-encoded Formats 输出 parquet文件
3. 目前 没有找到 类似 spark 读 parquet 文件 的类似方式 [在 flink 1.11.0 会释放出来](https://issues.apache.org/jira/browse/FLINK-16951)


## Checkpointing

## Source

### File-based  

#### readTextFile(path) / TextInputFormat - Reads files line wise and returns them as Strings.  

#### readTextFileWithValue(path) / TextValueInputFormat - Reads files line wise and returns them as StringValues. StringValues are mutable strings.  

#### readCsvFile(path) / CsvInputFormat - Parses files of comma (or another char) delimited fields. Returns a DataSet of tuples, case class objects, or POJOs. Supports the basic java types and their Value counterparts as field types.  

#### readFileOfPrimitives(path, delimiter) / PrimitiveInputFormat - Parses files of new-line (or another char sequence) delimited primitive data types such as String or Integer using the given delimiter.  

#### readSequenceFile(Key, Value, path) / SequenceFileInputFormat - Creates a JobConf and reads file from the specified path with type SequenceFileInputFormat, Key class and Value class and returns them as Tuple2<Key, Value>.  

### Collection-based  

#### fromCollection(Iterable) - Creates a data set from an Iterable. All elements returned by the Iterable must be of the same type. 
 
#### fromCollection(Iterator) - Creates a data set from an Iterator. The class specifies the data type of the elements returned by the iterator.  
  
#### fromElements(elements: _*) - Creates a data set from the given sequence of objects. All objects must be of the same type.  

#### fromParallelCollection(SplittableIterator) - Creates a data set from an iterator, in parallel. The class specifies the data type of the elements returned by the iterator.  

#### generateSequence(from, to) - Generates the sequence of numbers in the given interval, in parallel.  

### Generic  

#### readFile(inputFormat, path) / FileInputFormat - Accepts a file input format.  

#### createInput  

##### read from JDBC  

###### Batch By JDBCInputFormat  
1. 需要maven依赖 flink-jdbc_2.11、mysql-connector-java  
2. 编写代码时候需要 自己 指定 字段名称和类型  
[查看示例](./src/main/scala/com/yyb/flink10/batch/JDBC/ReadFromJDBCInputFormat.scala)
###### Stream By JDBCInputFormat  
[查看示例](./src/main/scala/com/yyb/flink10/DataStream/sink/JDBC/ReadFromInputFormat.scala)  
###### Flink table & sql Batch By JDBCInputFormat
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/batch/JDBC/BatchJDBCReadByInputformat2TableSource.scala)
###### Flink table & sql Batch By JDBCTableSource
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/batch/JDBC/BatchJobReadFromJDBCTableSource.scala)
###### Blink table & sql Batch By JDBCInputFormat
  
###### Blink table & sql Batch By JDBCTableSource
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/batch/JDBC/BlinkBatchReadFromJDBCTableSource.scala)
###### Flink table & sql Stream By JDBCInputFormat
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/DataStream/JDBC/StreamJDBCReadByInputformat2TableSource.scala)
###### Flink table & sql Stream By JDBCTableSource
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/DataStream/JDBC/StreamJobReadFromJDBCTableSource.scala)
###### Blink table & sql Stream By JDBCInputFormat
  
###### Blink table & sql Stream By JDBCTableSource
[查看示例](./src/main/scala/com/yyb/flink10/table/blink/DataStream/JDBC/ReadDataFromJDBCTableSource.scala)

## Sink

### 时间戳 指定 和 水印产生  
#### 时间戳 指定  
##### 直接在数据源中指定  
###### kafka  
#### 通过 TimestampAssigner 接口指定  
#### 水印产生  
##### 周期性水印  
周期型水印需要配合 ExecutionConfig.setAutoWatermarkInterval(...) 设置 时间 使用；  
需要实现 AssignerWithPeriodicWatermarks 接口
###### 内置的周期性水印内部实现 
####### AscendingTimestampExtractor  
AscendingTimestampExtractor产生的时间戳和水印必须是单调非递减的，用户通过覆写extractAscendingTimestamp()方法抽取时间戳。  
如果产生了递减的时间戳，就要使用名为MonotonyViolationHandler的组件处理异常，  
有两种方式：打印警告日志（默认）和抛出RuntimeException。  
单调递增的事件时间并不太符合实际情况，所以AscendingTimestampExtractor用得不多。  
####### BoundedOutOfOrdernessTimestampExtractor  
BoundedOutOfOrdernessTimestampExtractor产生的时间戳和水印是允许“有界乱序”的，  
构造它时传入的参数maxOutOfOrderness就是乱序区间的长度，而实际发射的水印为通过覆写extractTimestamp()方法提取出来的时间戳减去乱序区间，  
相当于让水印把步调“放慢一点”。这是Flink为迟到数据提供的第一重保障。  
当然，乱序区间的长度要根据实际环境谨慎设定，设定得太短会丢较多的数据，设定得太长会导致窗口触发延迟，实时性减弱。  
####### IngestionTimeExtractor  
IngestionTimeExtractor基于当前系统时钟生成时间戳和水印，其实就是Flink三大时间特征里的摄入时间了。  
##### 带标点水印，就是只提取有 event 时间的数据作为水印,没有 event 时间的数据，返回null水印     
需要实现 AssignerWithPunctuatedWatermarks 接口  
打点水印比周期性水印用的要少不少，并且Flink没有内置的实现。  
AssignerWithPunctuatedWatermarks适用于需要依赖于事件本身的某些属性决定是否发射水印的情况。  
我们实现checkAndGetNextWatermark()方法来产生水印，产生的时机完全由用户控制。上面例子中是收取到用户ID末位为0的数据时才发射。  

### StreamingFileSink format
StreamingFileSink 有2种 File Formats：
1. Row-encoded sink
2. Bulk-encoded sink
注意 在 使用 StreamingFileSink 的时候需要开启 Checkpointing。负责写的文件可能一直处于 in-progress 或者 pending 的状态。


#### Row-encoded sink 需要指定 写目录 和 Encoder：
这个 Encoder 只有一个 直接子类 SimpleStringEncoder 只是对数据做一个编码，如果要实现自己的 可以继承这个类
#### Bulk-encoded sink 需要也指定 写目录和 BulkWriter.Factory.
BulkWriter.Factory 有 3 个实现类 CompressWriterFactory, ParquetWriterFactory, SequenceFileWriterFactory
对应 三个 数据格式 (注意需要添加 maven flink 依赖)：
1. ParquetWriterFactory parquet格式 需要添加 flink-parquet 和 parquet-avro 依赖  
##### ParquetWriterFactory 产生的三种方式：
在生成 ParquetWriterFactory 的时候一种有3中方法  
    1) forSpecificRecord(Class<T> type) 这种方式传入一个 class ；  
    2) forGenericRecord(Schema schema)  这种方式传入的是一个 avro 包里面的 schema。在生产 avro 的 schema 的时候注意  
        需要用到 avro 的 抽象类 Schema 的 静态方法 createRecord 来产生 schema 对象。  
        注意对应的在 table 转化为 dataStream 对象的时候 也需要用到 toAppendStream(Table table, TypeInformation<T> typeInfo)和  
        GenericRecordAvroTypeInfo 这个类来完成转换，否则将会出现 类转化异常  
        这个需要maven加入 flink-avro 依赖。  
        经过测试，这种方式也是不行的！！！  最后通过 Apache Flink 中文用户邮件列表 提问才解决了。  
    3) forReflectRecord(Class<T> type)  这种方式传入也一个 class ；  
    4) [例子可见](./src/main/scala/com/yyb/flink10/table/blink/DataStream/FileSystem/ReadFromKafkaConnectorWriteToLocalParquetFileJava.java)  
    5) 为什么会对这个方法做这么多说明，因为如果我们想要做一些比较通过的程序，那么势必不应该处处使用到固定的 类， 
        所以动态根据 配置文件产生 shema的方式 就 显得非常重要了。当然你也可以在运行过程中，利用 ASM 等技术动态产生 class 类对象并加载；  
        不过在 分布式运行环境下比较难在与master和worker之间的 动态类 共享使用问题。  
2. Hadoop SequenceFileWriterFactory 需要添加依赖 flink-sequence-file、hadoop-client、flink-hadoop-compatibility  
3. SequenceFileWriterFactory supports additional constructor parameters to specify compression settings  

### StreamingFileSink RollingPolicy(滚动策略，文件轮替)
1. DefaultRollingPolicy
2. OnCheckpointRollingPolicy

DefaultRollingPolicy 可以设置三个 策略条件：
RolloverInterval 当前文件 早于 滚动间隔；
InactivityInterval 当前没有数据写到文件超过非活动的时间 默认 60S；
MaxPartSize 这个文件的大小，默认 128M； 

OnCheckpointRollingPolicy 的 滚动执行只会在 每一次 checkpoint 的时候。

注意这2个类都实现了 flink 的 RollingPolicy 接口，但是这个接口的实现有3个 DefaultRollingPolicy、OnCheckpointRollingPolicy、CheckpointRollingPolicy。
其中 CheckpointRollingPolicy 是抽象类，而 OnCheckpointRollingPolicy 又是 CheckpointRollingPolicy抽象类的实现。

因为 flink DataStream 不像spark DataStream 一样是 微批处理模式，不会产生 小文件，所以这里如果不指定 滚动策略，那么可能都在文件中追加内容 ？ 。

### StreamingFileSink BucketAssigner(输出文件 名称 指定 匹配模式)
1. 默认的是 DateTimeBucketAssigner 这种方式，即以时间格式 yyyy-MM-dd--HH，可以自己修改 以时间格式 和 时区
2. BasePathBucketAssigner 这种方式就是 不会指定 子文件的命名方式。

这2个类都是非 final的，所以可以继承用来实现自己的BucketAssigner。  
通过 调用 .withBucketAssigner(assigner) on the format builders.

这一部分是配置 输出文件的 前后缀的：
可以是调用 .withOutputFileConfig(config) 和 OutputFileConfig 结合 配置 输出文件的 前后缀的。  

### Sinks
#### JDBCSink
##### Batch By JDBCOutputFormat
[查看示例](./src/main/scala/com/yyb/flink10/DataSet/JDBC/WriteToMysqlByOutputformat.scala)  
##### Stream By JDBCOutputFormat  
[查看示例](./src/main/scala/com/yyb/flink10/DataStream/sink/JDBC/WriteToMysqlByJDBCOutputformat.scala)  
##### Flink table & sql Batch By JDBCAppendTableSink
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/DataSet/JDBC/WriteJDBCByTableSink.scala)
##### Flink table & sql Stream By JDBCAppendTableSink
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/DataSet/JDBC/WriteDataByTableSink.scala)
##### Blink table & sql Batch By JDBCAppendTableSink
[查看示例](./src/main/scala/com/yyb/flink10/table/blink/DataSet/JDBC/BlinkBatchWriteToJDBCTableSink.scala)
##### Blink table & sql Stream By JDBCAppendTableSink
[查看示例](./src/main/scala/com/yyb/flink10/table/blink/DataStream/JDBC/WriteDataByJDBCTableSink.scala)  

#### Kafka  
因为kafka没有对应的 OutputFormat，所以我们必须自己实现 KafkaOutputFormat。  
[查看示例](./src/main/scala/com/yyb/flink10/OutputFormat/KafkaOutputFormat.java)  
因为kafka没有对应的 BatchTableSink，所以我们必须自己实现 KafkaBatchTableSink。  
[查看示例](./src/main/scala/com/yyb/flink10/sink/KafkaBatchTableSink.java)  
因为 Blink Batch 模式下 DataSet 和 Table 不能相互转化，所以 write to kafka 就不做示例了。   
##### DataSet write to kafka  
[查看示例](./src/main/scala/com/yyb/flink10/DataSet/kafka/SendData2KafkaByKafkaOutputFormat.scala)  
##### DataStream write to kafka  
[查看示例](./src/main/scala/com/yyb/flink10/DataStream/kafka/SendData2KafkaByKafkaConnector.scala)  
##### flink batch table write to kafka  
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/batch/kafka/SendData2KafkaByKafkaBatchSink.scala)  
##### flink stream table write to kafka  
[查看示例](./src/main/scala/com/yyb/flink10/table/flink/stream/kafka/SendData2KafkaByKafkaConnector.scala)  
##### blink stream table write to kafka  
[查看示例](./src/main/scala/com/yyb/flink10/table/blink/stream/kafka/WriteToKafkaByKafkaConnector.java)  
#### Elasticsearch  
##### Maven Dependency
```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch7_2.11</artifactId>
    <version>${flink.version}</version>
    <scope>${slef.scope}</scope>
</dependency>
```   



## Table & SQL
注意 Blink 和 Flink 在 Table&SQL 中的区别：
1. <span id='reson1' >Blink DataSet 是 DataStreaming 的特例，所以 table 和 dateset 之间的转化 是不支持的。</span>
2. Blink 不支持 BatchTableSource，可以使用 bounded StreamTableSource 代替。
3. Blink 只支持 Catalog，并且不再支持 ExternalCatalog。
4. FilterableTableSource 的实现 对于 old flink planner 和 Blink 是不兼容的；old flink planner 把 PlannerExpressions 下推到 FilterableTableSource； Blink 则下推到 Expressions。
5. key-value 的 config options 只对于 Blink 使用。
6. PlannerConfig 的实现 在这 2 种 是不一样的。
7. Blink 优化多个 sink 对于一个DAG（只有 TableEnvironment， 不支持 StreamTableEnvironment ）；old flink planner 总是优化 每个 sink 在新的 DAG。
8. old flink planner 不再支持 catalog statistics，Blink 则支持。

### TableConfig  
可以通过 TableConfig 配置 state 的过期时间等等  

### Flink Batch Table
1. 可以从 DataSet 转化到 Table
2. 可以把 Table 转化到 DataSet

### Flink Stream Table
1. 可以从 DateStream 转化到 Table
2. 可以把 Table 转化到 DateStream

### Blink Batch Table
1. 不能 DataSet 转化到 Table
2. 目前还未找到 Table 转化为 DataSet的 方式  [原因点击查看,位于 Table & SQL 的 注意 第一条](#reson1)
3. hive 操作。
#### Blibk Hive DDL
[DDL to create Hive tables, views, partitions, functions within Flink will be supported soon.](#https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/hive/#ddl)
#### Blibk Hive DMl

### Blink Stream Table
1. 可以从 DateStream 转化到 Table
2. 可以把 Table 转化到 DateStream
3. hive
4. hive parquet

#### Blink 和 Hive 集成
1. 依赖 flink-connector-hive_2.11、hive-exec、flink-table-api-java-bridge_2.1、datanucleus-api-jdo、javax.jdo、datanucleus-rdbms、derby、mysql-connector-java  
注意 在解决 maven依赖的时候 需要仔细，可以提示配置的 版本没有 需要的方法，请注意修改版本，上面的依赖都是本人一步步慢慢解决出来的。
2. 依赖 hive-conf/hive-site.xml，如果本地需要的话，需要下载到 resources 里面，并且需要配置其中的 datanucleus.schema.autoCreateAll 为 true
3. 在 new  HiveCatalog 时的 hiveConfDir 参数时候，请注意 配置到文件，不能指定 null。[可参见代码](./src/main/scala/com/yyb/flink10/table/blink/DataSet/BlinkHiveBatchDemo.scala)

### Connect to External Systems
#### Filesystem
#### Elasticsearch
注意有的时候，需要排除 jackson 低版本的依赖：   
```
NoSuchFieldError: FAIL_ON_SYMBOL_HASH_OVERFLOW问题解决
```   
当发现 数据无法 插入到 es的时候，可以 env.disableOperatorChaining(); 看看有什么错误！！！  
  

#### Apache Kafka
1. 需要添加依赖 
        <dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-sql-connector-kafka-${kafka.version}_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
			<scope>compile</scope>
		</dependency>
2. flinkTableEnvrionment connect kafka
[代码可见](./src/main/scala/com/yyb/flink10/table/blink/DataStream/kafka/ReadDataFromKafkaConnectorJava.java)  
#### HBase
#### JDBC
##### Append Only like insert
[可以参考这个博文](https://www.jianshu.com/p/c352d0c4a458)  
[代码可见](./src/main/scala/com/yyb/flink10/table/flink/stream/JDBC/InsetMode/AppendOnly.java)  
##### Retract Stream  like delete + insert
[代码可见](./src/main/scala/com/yyb/flink10/table/flink/stream/JDBC/InsetMode/RetractStream.java)  
##### Upsert Stream  like 一般是 upadte  
The main difference to a retract stream is that UPDATE changes are  
encoded with a single message and are therefore more efficient.   
[代码可见](./src/main/scala/com/yyb/flink10/table/flink/stream/JDBC/InsetMode/UpsertStream.java)  

### join  
#### 双流join  
[参考文档](#http://www.360doc.com/content/19/0904/17/14808334_859110520.shtml)  
不论是INNER JOIN还是OUTER JOIN 都需要对左右两边的流的数据进行保存，JOIN算子会开辟左右两个State进行数据存储，左右两边的数据到来时候，进行如下操作：  
1. LeftEvent到来存储到LState，RightEvent到来的时候存储到RState；  
2. LeftEvent会去RightState进行JOIN，并发出所有JOIN之后的Event到下游；  
3. RightEvent会去LeftState进行JOIN，并发出所有JOIN之后的Event到下游  

## State
### Using Managed Keyed State  
[代码可见](./src/main/scala/com/yyb/flink10/DataStream/State/StateOfCountWindowAverage.java)  
#### KeyedProcessFunction + 延迟触发  
KeyedProcessFunction 在内部使用了 Keyed State。  
[代码可见](./src/main/scala/com/yyb/flink10/DataStream/ProcessFunction/KeyedProcessFunctionDemo.java)   
#### ProcessFunction + 延迟触发  
[代码可见](./src/main/scala/com/yyb/flink10/DataStream/ProcessFunction/KeyedProcessFunctionDemo.java)  

### Using Managed Operator State(no-key 算子状态)   
注意 processFucntion 只能适用于 Keyed State    
#### CheckpointedFunction  
#### ListCheckpointed  
#### Stateful Source Functions 保证 source 的  exactly-once  

## 问题
### 从 flink 官网使用 maven 初始化的项目 问题
1. idea 本地运行 提示 缺包问题。修改 pom文件 dependency 的 scope 范围，可以直接注释掉这个 选项
### DataSet 程序 有的 地方不执行的问题
1. DataSet 程序 有的地方没有执行，可能你的程序 最后没有调用 env.execution() 方法  
2. 目前来看，只有在 有 sink的情况下，需要 加 env.execution() 方法  
### 在自己的 JOb 后面有 env.execution() 的时候，有时候运行JOb会保存  
这个原因是，只有在有 Sink 的时候，才需要调用 env.execution() 这个方法。  
### 当 JDBC 这些 有界表 作为 维度表 使用 tableSource 使用的时候， 可能会发生 checkPoint 失败的，  
所以 需要注意，在 flink 的 流处理中  维度表 不要使用 有界数据源；  
无界数据源 做 维度表 是 完全符合 要求的！！！  


## Flink10 BUG
### StreamingFileSink build bug
当使用 StreamingFileSink 的多个 with... 方法时，会提示
 
      Error:xxx value build is not a member of ?0
      possible cause: maybe a semicolon is missing before `value build'?
      build()
      
请注意，这是 flink StreamingFileSink scala 版本的是一个 bug，可以使用 java 版本编写，或者使用 更高的 flink 版本。

详情见：https://issues.apache.org/jira/browse/FLINK-16684
### HiveCatalog hiveConfDir Bug
当使用 resources 目录下的 hive-site.xml 配置文件时，需要指定 hiveConfDir 的目录，且不能为null，
否则会出现一下错误：

    Required table missing : "DBS" in Catalog "" Schema "". DataNucleus requires this table to perform its persistence operations. Either your MetaData is incorrect, or you need to enable "datanucleus.schema.autoCreateTables"
    org.datanucleus.store.rdbms.exceptions.MissingTableException: Required table missing : "DBS" in Catalog "" Schema "". DataNucleus requires this table to perform its persistence operations. Either your MetaData is incorrect, or you need to enable "datanucleus.schema.autoCreateTables"
    	at org.datanucleus.store.rdbms.table.AbstractTable.exists(AbstractTable.java:606)
    	at org.datanucleus.store.rdbms.RDBMSStoreManager$ClassAdder.performTablesValidation(RDBMSStoreManager.java:3385)

解决方式就是 指定 hiveConfDir 目录。[可参见代码](./src/main/scala/com/yyb/flink10/table/blink/DataSet/BlinkHiveBatchDemo.scala)  
### kafka table source 转化为 dataStream 增加 rowtime 问题  
当使用 kafka table source 转化为 dataStream 增加 rowtime 的时候，在运行的时候，  
会出现 空指针 异常的错误，那么只能使用 kafkaConsumer 了。  
### 使用 WaterMark 时候 UI界面一直没有Watermark 产生  
可能的原因就是 使用的方式不对，必须在操作 数据的时候 链式编程指定 assignTimestampsAndWatermarks：  
```
DataStream<Current1> streamOrder = streamOrderStr.map(new MapFunction<String, Current1>() {
            @Override
            public Current1 map(String value) throws Exception {
                Current1 current1 = JSON.parseObject(value, Current1.class);
                return current1;
            }
        }).assignTimestampsAndWatermarks(new TimestampExtractorOrder(Time.seconds(0)));
```   
而不是 变量.xx 的方法指定 assignTimestampsAndWatermarks：   
```
DataStream<Current1> streamOrder = streamOrderStr.map(new MapFunction<String, Current1>() {
               @Override
               public Current1 map(String value) throws Exception {
                   Current1 current1 = JSON.parseObject(value, Current1.class);
                   return current1;
               }
           });
streamOrder.assignTimestampsAndWatermarks(new TimestampExtractorOrder(Time.seconds(0)));
```   
