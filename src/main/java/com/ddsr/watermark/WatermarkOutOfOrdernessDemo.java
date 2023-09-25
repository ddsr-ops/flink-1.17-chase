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
 * Out of Orderness Watermark
 *
 * @author ddsr, created it at 2023/8/26 11:38
 */
public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        /**
         * 演示watermark在多并行度下的传递
         * 1. task接收到多个上游watermark，取最小
         * 2. task往下游子任务以广播形式发送task当前watermark
         *
         * In case of parallelism one, WaterSensor s1,8,8 arrived, which trigger the window closed and computed.
         * However, in case of parallelism two, WaterSensor s1,8,8 arrived, which can not trigger computing.
         * When s1,9,9 arrived, trigger the window closed and computed.
         *
         * s1,1,1
         * s1,2,2
         * s1,8,8
         * s1,1,1
         * s1,2,2
         * s1,8,8  one partition(socket source rebalance)   == not trigger, watermark = 2 - 3 = -1
         * s1,8,8  another partition == trigger, watermark = 8 - 3 = 5
         * s1,13,13  == not trigger
         * s1,13,13  == trigger
         *
         * s1,1,1
         * s1,8,8   == not trigger
         * s1,9,9   == trigger
         *
         */
        env.setParallelism(2);

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
 * Inner watermark generation strategy
 *  It is generated periodically, with the duration of 200 ms
 *  Ascending stream:  watermark = the max time of events - 1 ms
 *  OutOfOrderness Stream: watermark = the max time of events - delay time - 1 ms
 *
 */