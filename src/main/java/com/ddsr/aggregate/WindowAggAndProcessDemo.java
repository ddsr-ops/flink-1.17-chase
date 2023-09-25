package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * Use ProcessWindowFunction(or WindowFunction) in conjunction with AggregateFunction
 *
 * @author ddsr, created it at 2023/8/25 20:43
 */
public class WindowAggAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /**
         * **
         *          * 增量聚合 Aggregate + 全窗口 process
         *          * 1、增量聚合函数处理数据： 来一条计算一条
         *          * 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数
         *          * 3、经过全窗口函数的处理包装后，输出
         *          *
         *          * 结合两者的优点：
         *          * 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         *          * 2、全窗口函数： 可以通过 上下文 实现灵活的功能
         */
        sensorWS.aggregate(new WaterSensorAgg(), new WaterSensorProcess())
                .print();

        env.execute();
    }

    public static class WaterSensorAgg implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return value.getVc() + accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    // 全窗口函数的输入类型 = 增量聚合函数的输出类型
    public static class WaterSensorProcess extends ProcessWindowFunction<Integer, String, String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<String> out) {
            // Get the start and end of the window, then format them to string "yyyy-MM-dd HH:mm:ss.SSS"
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startStr = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String endStr = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
            long count = elements.spliterator().estimateSize();
            // elements size = 1
//            Integer i = elements.iterator().next(); // Only invoke next() once
            for (Integer element : elements) {
                out.collect("key: " + key + ", count: " + count + ", start: " + startStr + ", end: " + endStr + ", vc: " + element);
            }

        }
    }
}