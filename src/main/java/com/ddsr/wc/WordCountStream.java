package com.ddsr.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * DataStream API read txt file (bounded stream)
 *
 * What about unbounded stream , such as socket stream, kafka stream(not specify end offset).
 *
 * @author ddsr, created it at 2023/8/5 21:49
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment, automatically resolve running env: local or remote
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // If you use IDEA to develop flink programs, use this env to launch WEB UI
        // Prerequisites: flink-runtime-web dependency included
        // Default parallelism is thread number of PC if parallelism is not specified.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.readTextFile("input/word.txt")
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
