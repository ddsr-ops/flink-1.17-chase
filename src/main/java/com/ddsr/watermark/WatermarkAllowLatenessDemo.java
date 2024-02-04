package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * When it comes to supporting event time, Flink’s streaming runtime builds on the pessimistic assumption that events
 * may come out-of-order, i.e. an event with timestamp t may come after an event with timestamp t+1. Because of this,
 * the system can never be sure that no more elements with timestamp t < T for a given timestamp T can come in the
 * future. To amortise the impact of this out-of-orderness on the final result while making the system practical, in
 * STREAMING mode, Flink uses a heuristic called Watermarks. A watermark with timestamp T signals that no element
 * with timestamp t < T will follow.
 * <p>
 * Allow lateness
 *
 * @author ddsr, created it at 2023/8/26 11:38
 */
public class WatermarkAllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        // define an out-of-orderness watermark strategy, extracting field ts of WaterSensor as watermark
        WatermarkStrategy<WaterSensor> outOfOrdernessWatermark = WatermarkStrategy
                // out of orderness, wait for another 3 seconds
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // Extract timestamp from WaterSensor
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                           @Override
                                           public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                               System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                               return element.getTs() * 1000L; // milliseconds in unit
                                           }
                                       }
                );

        sensorDS.assignTimestampsAndWatermarks(outOfOrdernessWatermark)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // event window of 5 seconds, corresponds to event time characteristic(default)
                .allowedLateness(Time.seconds(2)) // only valid for event time window, this waits for 2 seconds of event time, not processing time
                // In this example of parallelism one , when watermark is going on at (s1,10,10), the event window [0, 5) will be closed
                // because allowLateness 2 + OutOfOrderness 3 = 5 has elapsed.
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                })
                .print();


        env.execute();
    }

}


/**
 * OutOfOrderness: 事件时间小的事件，比事件时间大的事件晚来
 * Lateness: 事件的事件事件小于watermark
 *
 *  Flink的窗口，也允许迟到数据。当触发了窗口计算后，会先计算当前的结果，但是此时并不会关闭窗口。
 * 以后每来一条迟到数据，就触发一次这条数据所在窗口计算(增量计算)。直到wartermark 超过了窗口结束时间+推迟时间，此时窗口会真正关闭。
 */