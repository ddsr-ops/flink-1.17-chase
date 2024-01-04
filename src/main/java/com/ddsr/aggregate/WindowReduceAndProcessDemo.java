package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * Process Function is engaged in conjunction with Reduce Function.
 * <p></p>
 *
 * Process Functions aim to perform the every elements processing in the window; Reduce Function aims to perform the
 * incremental aggregation in the window.
 * <p></p>
 *
 * The reduce function distinguishes from the aggregate function. The reduce function only returns a value the same type
 * as input values, and the aggregate function can return a value of any type.
 *
 * @see WindowAggAndProcessDemo
 * @author ddsr, created it at 2024/1/4 20:23
 */
public class WindowReduceAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        sensorWS.reduce(new MyReducingMax(), new MyWindowFunction())
                .print();

        env.execute();
    }

    private static class MyReducingMax implements ReduceFunction<WaterSensor> {
        public WaterSensor reduce(WaterSensor r1, WaterSensor r2) {
            return r1.getVc() > r2.getVc() ? r1 : r2;
        }
    }

    private static class MyWindowFunction extends ProcessWindowFunction<
                WaterSensor, Tuple3<String, Long, WaterSensor>, String, TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<WaterSensor> maxReading,
                Collector<Tuple3<String, Long, WaterSensor>> out) {

            WaterSensor max = maxReading.iterator().next();
            out.collect(Tuple3.of(key, context.window().getEnd(), max));
        }
    }
}
