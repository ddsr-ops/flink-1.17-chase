package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Monotonous Watermark
 *
 * @author ddsr, created it at 2023/8/26 11:38
 */
public class WatermarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        // define a monotonous watermark strategy, extracting field ts of WaterSensor as watermark
        // 按理来说使用monotonous水位线，不应该有乱序数据；如果乱序的数据来了，其对应窗口已经关闭，不予处理
        WatermarkStrategy<WaterSensor> monotonousWatermark = WatermarkStrategy
                // Monotonous
                .<WaterSensor>forMonotonousTimestamps()
                // Extract timestamp from WaterSensor
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                           @Override
                                           public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                               System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                               return element.getTs() * 1000L; // milliseconds in unit
                                           }
                                       }
                );

        sensorDS.assignTimestampsAndWatermarks(monotonousWatermark)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // event window of 5 seconds, corresponds to event time characteristic(default)
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
