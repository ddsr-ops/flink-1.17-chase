package com.ddsr.value.types;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * see {@link IntValueProcessFunction}
 * @author ddsr, created it at 2024/12/19 13:49
 */
public class IntValueProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);

        intStream.process(new IntValueProcessFunction()).print();

        env.execute();
    }
}
