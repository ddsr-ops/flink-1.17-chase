package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Apply function for all window stream of non-keyed datastream
 *
 * @author ddsr, created it at 2025/1/6 18:04
 */
@SuppressWarnings({"unused", "Convert2Lambda"})
public class WindowAllApplyDemo {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("192.168.20.126", 7777)
                .map(new WaterSensorMapFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        // one parallelism
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new AllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<WaterSensor> values, Collector<String> out) {
                        int sum = 0;
                        for (WaterSensor value : values) {
                            sum += 1;
                        }
                        out.collect("count: " + sum);
                    }
                })
                .print();


        env.execute("WindowAllApplyDemo");
    }

}
