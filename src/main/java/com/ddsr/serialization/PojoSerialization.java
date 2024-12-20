package com.ddsr.serialization;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The PojoTypeInfo is creating serializers for all the fields inside the POJO. Standard types such as int, long, String
 * etc. are handled by serializers we ship with Flink. For all other types, we fall back to Kryo.
 *
 * @author ddsr, created it at 2024/12/20 16:02
 */
public class PojoSerialization {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // If Kryo is not able to handle the type, you can ask the PojoTypeInfo to serialize the POJO using Avro. To
        // do so, you have to call

        env.getConfig().enableForceAvro();

        env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        ).print();

        env.execute();

    }
}
