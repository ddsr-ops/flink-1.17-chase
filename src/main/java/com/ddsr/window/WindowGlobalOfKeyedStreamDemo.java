package com.ddsr.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * A global windows assigner assigns all elements with the same key to the same single global window. This windowing
 * scheme is only useful if you also specify a custom trigger. Otherwise, no computation will be performed, as the
 * global window does not have a natural end at which we could process the aggregated elements.
 *
 * @author ddsr, created it at 2025/1/24 10:50
 */
@SuppressWarnings("Convert2Lambda")
public class WindowGlobalOfKeyedStreamDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        // nc -lk 7777
        /*
         * 1,1
         * 1,11  key=1的窗口包含1 11
         * 2,2
         * 3,3
         * 2,22  key=2的窗口包含2 22
         */

        env.socketTextStream("88.88.16.196", 7777)
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) {
                        String[] split = value.split(",");
                        String key = split[0];
                        String value1 = split[1];
                        out.collect(Tuple2.of(key, value1));
                    }
                })
                .keyBy(e -> e.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(2)) // trigger the window for every 2 elements
                .apply(new WindowFunction<Tuple2<String, String>, String,
                        String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, String>> input,
                                      Collector<String> out) {

                        // concatenate the values of all the elements in the window into a String and print it out
                        StringBuilder sb = new StringBuilder();
                        for (Tuple2<String, String> element : input) {
                            sb.append(element.f1).append(" ");
                        }
                        out.collect("key=" + s + "的窗口包含" + sb);
                    }
                }).print();


        env.execute();
    }
}
