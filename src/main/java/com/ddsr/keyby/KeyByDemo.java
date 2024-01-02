package com.ddsr.keyby;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/18 22:14
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        /*
          1. returned type is KeyedStream
          2. KeyBy is not transformation operator, cat set parallelism
          3. partition by  and group by
             keyby 对数据分组， 保证相同key的数据， 在同一个分区；
             分区：一个子任务可以理解为一个分区， 一个分组内可存在多个分组key
             一个分区内， 可有多组(key)；一个key

           Every keyBy causes a network shuffle that repartitions the stream. In general this is pretty expensive,
           since it involves network communication along with serialization and deserialization.

           The keys must be produced in a deterministic way, because they are recomputed whenever they are needed,
            rather than being attached to the stream records.
         */

        // 方式一：使用Lambda表达式
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(e -> e.id);

        // 方式二：使用匿名类实现KeySelector
//        KeyedStream<WaterSensor, String> keyedStream1 = stream.keyBy(new KeySelector<WaterSensor, String>() {
//            @Override
//            public String getKey(WaterSensor e) throws Exception {
//                return e.id;
//            }
//        });

        keyedStream.print();
        env.execute();
    }
}
