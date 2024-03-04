package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Custom periodic watermark
 *
 * @author ddsr, created it at 2023/8/26 16:42
 */
public class WatermarkPeriodicCustomDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000L); // control how frequency to emit watermark, in milliseconds

        SingleOutputStreamOperator<WaterSensor> ds = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        // Refer to class WatermarkStrategy for more details about how to define a custom watermark strategy
        SingleOutputStreamOperator<WaterSensor> ds1 = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new MyPeriodicWatermarkGenerator<>(3000L);
                            }
                        })
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));
        ds1
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements);
                    }
                }).print();

        env.execute();

    }

    public static class MyPeriodicWatermarkGenerator<WaterSensor> implements WatermarkGenerator<WaterSensor> {

        // The maximum timestamp
        private long maxTs;
        // The out-of-orderness
        private final long outOfOrderness;

        public MyPeriodicWatermarkGenerator(long outOfOrderness) {
            this.outOfOrderness = outOfOrderness;
            this.maxTs = Long.MIN_VALUE + outOfOrderness + 1;
        }

        // Invoke this method when every event comes
        // eventTimestamp is the timestamp of the event
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            System.out.println("Invoke the onEvent method, current maxTs=" + maxTs + ", eventTimestamp=" + eventTimestamp + ", event=" + event + ".");
        }

        // invoke it periodically, emit watermark
        // the period is controlled by setAutoWatermarkInterval method
        // A new watermark will be emitted if the returned watermark is non-null and larger than the previous watermark.
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            Watermark watermark = new Watermark(maxTs - outOfOrderness - 1);
            output.emitWatermark(watermark);
            System.out.println("Invoke the onPeriodicEmit method, current watermark=" + watermark);
        }
    }

}


