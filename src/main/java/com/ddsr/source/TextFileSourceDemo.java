package com.ddsr.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2024/10/29 8:52
 */
public class TextFileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile("input/word.txt").print();

//        env.readTextFile("input/word.txt", "utf-8").print();

        env.execute();
    }
}
