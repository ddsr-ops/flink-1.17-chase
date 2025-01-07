package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Apply function for keyed windowed stream.
 * <p>
 * Applies the given window function to each window. The window function is called for each
 * evaluation of the window for each key individually. The output of the window function is
 * interpreted as a regular non-windowed stream.
 *
 * <p>Note that this function requires that all data in the windows is buffered until the window
 * is evaluated, as the function provides <strong>no means of incremental aggregation</strong>
 *
 * @author ddsr, created it at 2025/1/4 20:07
 */
@SuppressWarnings({"unused", "Convert2Lambda"})
public class WindowApplyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("192.168.20.126", 7777)
                .map(new WaterSensorMapFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<WaterSensor> input,
                                      Collector<String> out) {
                        int sum = 0;
                        for (WaterSensor value : input) {
                            sum += 1;
                        }
                        out.collect(key + " count: " + sum);
                    }
                })
                .print();

        env.execute();

    }
}
