package com.ddsr.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 *
 * Window Interval Join
 *
 * @author ddsr, created it at 2023/9/3 16:26
 */
public class WindowIntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        // todo: extract the created env to a static method
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1,1),
                        Tuple3.of("a", 11,1),
                        Tuple3.of("b", 2,1),
                        Tuple3.of("b", 5,1),
                        Tuple3.of("c", 14,1),
                        Tuple3.of("d", 15,1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );

        // Before using interval join, performs the keyBy operation firstly, selected Keys are the join keys
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(t -> t.f0);

        SingleOutputStreamOperator<String> intervalJoin = ks1.intervalJoin(ks2)
                // leftElement.timestamp + lowerBound <= rightElement.timestamp <= leftElement.timestamp + upperBound
                // basing on the left stream, boundaries are created
                .between(Time.seconds(-2), Time.seconds(2))
                .lowerBoundExclusive() // The lower bound is exclusive
                .upperBoundExclusive() // The upper bound is exclusive
                // When two streams are joined together, the process function is called
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    // The left element and the right element must have been joined together
                    // 意思， 能关联上的数据才会进入这个方法
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<String> out) {
                        out.collect(left + "<----->" + right);
                    }
                });

        intervalJoin.print();

        env.execute();
    }
}
