package com.yyb.flink10.DataStream.ProcessFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-05
 * @Time 09:23
 */
public class OperatorProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        env.setNumberOfExecutionRetries(3);

        String checkPointPath = "./checkPointPath/";
//        env.setStateBackend(new FsStateBackend(checkPointPath));
//        env.setStateBackend(new RocksDBStateBackend(checkPointPath));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.10.19:9092,172.16.10.26:9092,172.16.10.27:9092");
        properties.setProperty("group.id", "yyb");

        DataStreamSource<Tuple2<Long, Long>> soure = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(2L, 2L),
                Tuple2.of(2L, 4L)

        );

        // 注意当使用 延时触发的时候，必须使用 无界 的 source，因为 有界的 source，运行完成之后 就会 退出了，不会等待 延时的任务
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>("eventsource_yyb", new SimpleStringSchema(), properties);
        kafkaSource.setStartFromLatest();

        DataStreamSource<String> sourceKafka = env.addSource(kafkaSource);
        SingleOutputStreamOperator<Tuple2<Long, Long>> dataSource = sourceKafka.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
                String[] a  = value.split(",");
                return new Tuple2<Long, Long>(Long.parseLong(a[0]), Long.parseLong(a[1]));
            }
        });


        dataSource.process(new ProcessFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            private ValueState<Long> timeState;
            private ValueState<Tuple2<Long, Long>> msgState;
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 过期清理元素 设置
                    StateTtlConfig ttlConfigMsgState = StateTtlConfig
                            .newBuilder(Time.seconds(10))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();

                    StateTtlConfig ttlConfigTimeState = StateTtlConfig
                            .newBuilder(Time.seconds(20))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();

                    ValueStateDescriptor<Long> timeStateDesc = new ValueStateDescriptor<Long>("timeState", TypeInformation.of(Long.TYPE));
                    timeStateDesc.enableTimeToLive(ttlConfigTimeState);
                    timeState = getRuntimeContext().getState(timeStateDesc);

                    ValueStateDescriptor<Tuple2<Long, Long>> msgStateDesc = new ValueStateDescriptor<Tuple2<Long, Long>>("msgState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }));
                    msgStateDesc.enableTimeToLive(ttlConfigMsgState);
                    msgState = getRuntimeContext().getState(msgStateDesc);
                }

            //value 是进来的元素， out 是如果需要继续 从下游发数据就使用 out.collect 方法
               @Override
               public void processElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
                   System.out.println("processElement: " + value.toString());
                    if(value.f1 < 5){ //小于 5的话，延时 触发
                       Long triggerTime = ctx.timerService().currentProcessingTime() + 5000L; //5秒后延时 触发
                       timeState.update(triggerTime);
                       msgState.update(value);
                       ctx.timerService().registerProcessingTimeTimer(triggerTime);
                    }else{
//                        ctx.timerService().deleteProcessingTimeTimer(timeState.value());
//                        timeState.update(-1L);
                        out.collect(value);
                    }
               }

               //注意 这里 的 out 是发送到下游的算子的，
               // 当然也可以一个一个的在这里处理，不用发送到 下游算子
               @Override
               public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
                   if(timestamp == timeState.value()){
                       System.out.println("onTimer --- ");
                       out.collect(new Tuple2<Long, Long>(msgState.value().f0, msgState.value().f1 ));
                   }
               }
           }
        ).print().setParallelism(1);

        env.execute("Demo1");
    }
}
