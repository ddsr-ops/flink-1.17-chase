package com.ddsr.serialization;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2024/12/20 16:51
 */
public class UseWaterSensorSerializerDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // register custom serializer
        env.getConfig().addDefaultKryoSerializer(
                WaterSensor.class,
                WaterSensorSerializer.class
        );

        env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        ).print();

        env.execute();
    }
}
