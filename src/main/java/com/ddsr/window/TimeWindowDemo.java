package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Various time window demo
 *
 * @author ddsr, created it at 2023/8/22 22:26
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))); // Tumbling window
        // An important use case for offsets is to adjust windows to timezones other than UTC-0. For example, in
        // China you would have to specify an offset of Time.hours(-8).
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.hours(-8))); // Tumbling window
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))); // Sliding window, window length = 10, slide length = 5
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); // Session window, if the gap exceeds 5s, it is a window
//                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
//                    @Override
//                    public long extract(WaterSensor element) {
//                        return element.getTs() * 1000L;
//                    }
//                })); // Session window with dynamic gap, whenever the element arrives, the gap will be calculated by the extractor

        // All window process function (全窗口函数)
        // Begin to process them until all elements in the window arrive(meaning the window ends), only cache them before the window ends
        sensorWS
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * Processes the given elements within a window and collects the result.
                     *
                     * @param  key     the key associated with the elements
                     * @param  context the context object for the window
                     * @param  elements the elements within the window
                     * @param  out     the collector to collect the result
                     */
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        // Get the start and end of the window, then format them to string "yyyy-MM-dd HH:mm:ss.SSS"
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String startStr = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String endStr = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key: " + key + ", count: " + count + ", start: " + startStr + ", end: " + endStr);
                    }
                })
                .print();

        env.execute();

    }
}

/**
 * Trigger, Evictor, Various windows have their own implementations of them, which are implemented by ourselves.
 *
 * When time window trigger, such as time tumbling window?
 *   The time arrives at the end of the window - 1ms, the window will trigger
 *
 * How to make a time window?
 *   start = 向下取整，取窗口长度的整数倍
 *   end = start + window length
 *
 * Life cycle of time windows
 *   when to create: when the first event that belongs the window arrives, new a window singleton, next one belongs the same window, no need to create a new window
 *   when to destroy: when the last event that belongs the window arrives, destroy the window, also with consideration of delay duration
 *                    时间进展 >= 窗口的最大时间戳 -1ms + 允许迟到的最大时间（默认为0）
 */