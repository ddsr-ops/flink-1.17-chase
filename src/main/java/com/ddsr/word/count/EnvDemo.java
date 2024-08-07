package com.ddsr.word.count;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2023/8/12 22:24
 */
public class EnvDemo {
    @SuppressWarnings({"deprecation", "Convert2Lambda"})
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // rest web ui port
        configuration.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // Change runtime mode without modify operation api
        // STREAMING execution mode, can be used for both bounded and unbounded jobs.
        //        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Commonly, Not set runtime mode in codes, but use command-line option to set this via -Dexecution.runtime-mode=BATCH
        // The BATCH execution mode can only be used for Jobs/Flink Programs that are bounded.
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Change runtime mode automatically
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

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
