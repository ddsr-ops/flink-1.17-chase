package com.ddsr.word.count;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * DataSet API is not recommended
 *
 * @author ddsr, created it at 2023/8/5 20:18
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/word.txt")
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() { // Do not use lambda, if you insist on use lambda, refer to the following section
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
//                        for (String word : s.split(" ")) {
//                            collector.collect(Tuple2.of(word, 1));
//                        }
//                    }
//                })
                .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String word : s.split(" ")) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // Types.TUPLE(Types.STRING, Types.INT, Types.INT)
                .groupBy(0)// the first position
                .sum(1) // the second position
                .print();

    }
}
