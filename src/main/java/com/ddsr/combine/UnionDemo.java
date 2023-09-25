package com.ddsr.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/19 20:31
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> ds2 = env.fromElements(2, 2, 3);
        DataStreamSource<String> ds3 = env.fromElements("2", "2", "3");

        // 1. types of streams to union are the same
        // 2. union more than one streams once
        ds1.union(ds2,ds3.map(Integer::valueOf))
                .print("===");

        ds1.union(ds2).union(ds3.map(Integer::valueOf))
                .print("---");

        env.execute();
    }
}
