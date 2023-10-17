package com.ddsr.tranforms;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/10/17 21:40
 */
public class TransRichFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(

                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        stream.filter(
                new RichFilterFunction<WaterSensor>() {
                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("Enter the close method");
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("Enter the open method");
                        super.open(parameters);
                    }

                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getId().equals("sensor_1");
                    }
                }
        ).print();


        env.execute();
    }
}
