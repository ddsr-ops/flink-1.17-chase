package com.ddsr.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author ddsr, created it at 2023/8/19 20:43
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> source2 = env.fromElements("a", "b", "c");

        // 1. type of streams to connect may be different, distinguishing from union
        // 2. Only two streams can be connected
        // 3. Connected stream use map, process, flatmap , handle data from two streams separately
        SingleOutputStreamOperator<String> connectedStream = source1.connect(source2)
                .map(new CoMapFunction<Integer, String, String>() {
                    // for source1
                    @Override
                    public String map1(Integer value) throws Exception {
                        return value.toString();
                    }

                    // for source2
                    @Override
                    public String map2(String value) throws Exception {
                        return value;
                    }
                });
        connectedStream.print();

        env.execute();
    }
}
