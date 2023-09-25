package com.ddsr.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream API read unbounded stream (unbounded stream)
 *
 * What about unbounded stream , such as socket stream, kafka stream(not specify end offset).
 *
 * @author ddsr, created it at 2023/8/5 21:49
 */
public class WordCountStreamUnbounded {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // If you use IDEA to develop flink programs, use this env to launch WEB UI(http://localhost:8081)
        // Prerequisites: flink-runtime-web dependency included
        // Default parallelism is thread number of PC if parallelism is not specified.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // Keep nc running on the spark1 machine before starting this program: nc -lk 9999
        env.socketTextStream("spark1", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                        for (String word : s.split(" ")) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
