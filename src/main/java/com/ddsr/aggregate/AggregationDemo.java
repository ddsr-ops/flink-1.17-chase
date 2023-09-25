package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.MyKeySelector;
import com.ddsr.functions.MyReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/18 22:42
 */
public class AggregationDemo {
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

        /**
         * minBy()：与min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计算指定字段的最小值，
         * 其他字段会保留最初第一个数据的值；而minBy()则会返回包含字段最小值的整条数据。
         *
         * maxBy()：与max()类似，在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致。
         */
//        stream.keyBy(e -> e.id).max("vc").print();    // 指定字段名称， 只有元组类型， 才能指定位置
//        stream.keyBy(e -> e.id).maxBy("vc").print();    // 指定字段名称， 只有元组类型， 才能指定位置

        stream.keyBy(new MyKeySelector())
                .reduce(new MyReduceFunction())
                .print();
        env.execute();
    }
}
