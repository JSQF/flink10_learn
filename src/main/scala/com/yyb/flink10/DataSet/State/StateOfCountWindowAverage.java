package com.yyb.flink10.DataSet.State;

import com.yyb.flink10.commonEntity.Current1;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-03
 * @Time 14:52
 */
public class StateOfCountWindowAverage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> soure = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(2L, 2L),
                Tuple2.of(2L, 4L)

        );

//        soure.keyBy(0).flatMap(new CountWindowAverage())
//                .print();

        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> rs1 = soure.keyBy(0).flatMap(new GroupSum());
        rs1.print();


        env.execute("StateOfCountWindowAverage");


    }

    /**
     * 为什么要用 flatMap ,flatMap 有 抽取一层数据的意思，这个 把 2 个元素 计算 输出了 1个元素
     */
    static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>{
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple2<Long, Long> currentSum = sum.value();
            currentSum.f0 = currentSum.f0 + 1;
            currentSum.f1 = currentSum.f1 + value.f1;
            sum.update(currentSum);
            if(currentSum.f0 >=2){ //这里遇到 2 个 一组的元素 求均值后 ，输出 ，清空 sum；如果需要求这一组的 平均值，则不需要这里
                out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化 和 get sum ValueState
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                            Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);
        }
    }

    static class GroupSum extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>>{
        //这里好输出的类型对应
        private transient ValueState<Tuple3<Long, Long, Long>> sum;

        //第一个字段 输入的类型
        //第二个字段 输出的类型
        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
            Tuple3<Long, Long, Long> currentSum = sum.value();
            currentSum.f0 = value.f0;
            currentSum.f1 = currentSum.f0 + 1;
            currentSum.f2 = currentSum.f2 + value.f1;
            sum.update(currentSum);
            out.collect(Tuple3.of(currentSum.f0, currentSum.f1, currentSum.f2)); //注意这里每一条数据都会返回出去，所以这个不适合 RichFlatMapFunction 做 聚合的 操作的
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple3<Long, Long, Long>> descriptor = new ValueStateDescriptor<>(
                    "sum",
                    TypeInformation.of(new TypeHint<Tuple3<Long, Long, Long>>() { //和输出类型对应
                    }),
                    Tuple3.of(0L, 0L, 0L)
            );

            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
