package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * Process Window Function Demo
 *
 * @author ddsr, created it at 2023/8/22 22:26
 */
public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

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
