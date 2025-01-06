package com.ddsr.tranforms;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import com.ddsr.topn.ProcessAllWindowTopNDemo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * DataStream → AllWindowedStream
 * <p>
 * Windows can be defined on regular(non-keyed) DataStreams. Windows group all the stream events according to some
 * characteristic
 * (e.g., the data that arrived within the last 5 seconds).
 * <p>
 * <Strong>Note: This is in many cases a non-parallel transformation. All records will be gathered in one task for the
 * windowAll operator.</Strong>
 *
 * @author ddsr, created it at 2025/1/4 19:56
 */
public class WindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        // one parallelism
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowTopNDemo.MyTopNProcessAllWindowFunction())
                .print();

        env.execute();

    }
}
