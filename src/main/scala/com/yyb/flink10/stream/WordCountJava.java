package com.yyb.flink10.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-04-17
 * @Time 17:20
 */
public class WordCountJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("wordcount")
                .withPartSuffix("exe")
                .build();

        DataStream<String> text = null;
        if(params.has("--input")){
            text = env.readTextFile(params.get("--input"));
        }else{
            System.out.println("Executing WordCount example with default inputs data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new myFlatMap());
        SingleOutputStreamOperator<Tuple2<String, Integer>> rs = counts.keyBy(0).sum(1);

        StreamingFileSink streamingFileSink = StreamingFileSink.forRowFormat(new Path("./xxx.text"),
                new SimpleStringEncoder<Tuple2<String, Integer>>("utf-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .withBucketAssigner(new BasePathBucketAssigner<Tuple2<String, Integer>>() )
                .withOutputFileConfig(config)
                .build();

        rs.addSink(streamingFileSink);

        rs.setParallelism(1).print();

        env.execute("xxx");

    }
    public static final class myFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for(String token : tokens){
                if(token.length() >0 ){
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}

