package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Allow lateness and side output late data
 *
 * @author ddsr, created it at 2023/8/26 11:38
 */
public class WatermarkSideOutputLateDataDemo {
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

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorDS.assignTimestampsAndWatermarks(outOfOrdernessWatermark)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) // event window of 5 seconds, corresponds to event time characteristic(default)
                .allowedLateness(Time.seconds(2)) // only valid for event time window, this waits for 2 seconds of event time, not processing time
                // In this example of parallelism one , when watermark is going on at (s1,10,10), the event window [0, 5) will be closed
                // because allowLateness 2 + OutOfOrderness 3 = 5 has elapsed.
                .sideOutputLateData(lateTag)
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
                });
        process
                .print();

        process.getSideOutput(lateTag).printToErr();


        env.execute();
    }

}


/**
 * *2、乱序、迟到数据的处理
 * *1)watermark中指定乱序等待时间 ==> 针对DataStream
 * *2)如果开窗，设置窗口允许迟到 ==> 针对WindowedStream
 *   =》推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
 *   =》关窗后，迟到数据不会被计算
 * *3)关窗后的迟到数据，放入侧输出流 ==> 针对WindowedStream
 * *
 * *如果watermark等待3s，窗口允许迟到2s，为什么不直接 watermark等待5s或者窗口允许迟到5s ?
 * =》watermark等待时间不会设太大===》影响的计算延迟
 * *
 *    如果3s ==》窗口第一次触发计算和输出，13s的数据来。13-3=10s
 * *
 *    如果5s ==》窗口第一次触发计算和输出，15s的数据来。15-5=10s
 * *=》窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确
 * *
 *    如果只设置允许迟到5s ，那么就会导致频繁重新输出
 * *
 * ToDo设置经验
 * 1、watermark等待时间，设置一个不算特别大的，一般是秒级，在乱序和延迟取舍
 * 2、设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
 * 3、极端小部分迟到很久的数据，放到侧输出流。获取到之后可以做各种处理
 *
 *
 */