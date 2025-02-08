package com.ddsr.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * CoFlatMap Example
 *
 * @author ddsr, created it at 2025/2/8 17:00
 */
public class ConnectFlatMapDemo {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> stringDataStream = env.fromElements("1", "2", "3", "4", "5");

        integerDataStream.connect(stringDataStream)
                .flatMap(new CoFlatMapFunction<Integer, String, String>() {
                    @Override
                    public void flatMap1(Integer value, Collector<String> out) {
                        out.collect("source1 " + value);
                    }

                    @Override
                    public void flatMap2(String value, Collector<String> out) {

                        out.collect("source2 " + value);
                    }
                }).print();

        env.execute();

    }
}
