package com.ddsr.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2023/10/9 22:24
 */
public class RemoteEnvDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment(
                "host", // JobManager host
                8088,    // JobManager port
                "hdfs://path/xxx.jar" // Job jar file
        );

        remoteEnvironment.readTextFile("input/word.txt")
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

        remoteEnvironment.execute();

    }
}
