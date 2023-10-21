package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author ddsr, created it at 2023/10/21 21:23
 */
public class EvictorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_1", 2L, 3),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

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
        stream.assignTimestampsAndWatermarks(monotonousWatermark)
                .keyBy(e -> e.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .evictor(new MyEvictor())
                .process(new ProcessWindowFunction<WaterSensor, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, Object, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {
                        elements.forEach(out::collect);
                    }
                });
        env.execute();


    }

    public static class MyEvictor implements Evictor<WaterSensor, TimeWindow> {
        @Override
        public void evictBefore(Iterable<TimestampedValue<WaterSensor>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            // Implement your custom evict logic here
            if (size > 2) {
                Iterator<TimestampedValue<WaterSensor>> iterator = elements.iterator();
                iterator.next();
                iterator.next();
                while (iterator.hasNext()) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<WaterSensor>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            // Implement your custom evict logic here
            if (size > 2) {
                Iterator<TimestampedValue<WaterSensor>> iterator = elements.iterator();
                iterator.next();
                iterator.next();
                while (iterator.hasNext()) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }
    }
}