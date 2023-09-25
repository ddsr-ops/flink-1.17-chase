package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 *
 * CountWindow Demo
 *
 * @author ddsr, created it at 2023/8/26 8:26
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, GlobalWindow> countWindow = sensorKS
//                .countWindow(3); // count tumble window, size = 3, trigger computing when the third element arrives, then 6, 9, 12..
        .countWindow(4,2); // count sliding window, size = 4, slide length = 2, trigger computing when the second element arrives, then 4, 6, 8..
        // 每经过一个步长，都有一个窗口触发计算，窗口的长度为4

        countWindow.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {

            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long count = elements.spliterator().estimateSize();
                out.collect("key: " + key + ", count: " + count + " - " + elements);
            }
        }).print();

        env.execute();


    }
}
