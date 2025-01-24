package com.ddsr.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * window() api is designed for keyed stream, referring to
 * {@link org.apache.flink.streaming.api.datastream.KeyedStream}
 * windowAll() api is designed for non-keyed stream, referring to
 * {@link org.apache.flink.streaming.api.datastream.DataStream}
 *
 * @author ddsr, created it at 2025/1/24 10:11
 */
@SuppressWarnings("Convert2Lambda")
public class WindowGlobalDemo {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // nc -lk 7777
        /*
         * 1,2,3,4,5
         * 6,7,8,9,10 = print count: 10
         * 11,12,13,14,15
         * 16,17,18,19,20 = print count: 20
         */
        DataStreamSource<String> text = env.socketTextStream("192.168.20.126", 7777);

        SingleOutputStreamOperator<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        });

        // non-keyed stream
        words.windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(10))  // trigger the window for every 10 elements
                .process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, GlobalWindow>.Context context,
                                        Iterable<String> elements, Collector<String> out) {
                        int count = 0;
                        for (String ignored : elements) {
                            count++;
                        }
                        out.collect("count: " + count);
                    }
                })
                .print();

        env.execute();
    }
}
