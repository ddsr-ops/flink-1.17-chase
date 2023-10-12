package com.ddsr.tranforms;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2023/8/18 21:57
 */
public class TransFlatMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(

                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)

        );

        stream.flatMap(new MyFlatMap()).print();

        stream.flatMap(new MyFlatMap(), Types.STRING).printToErr();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<WaterSensor, String> {

        // flatmap: one to zero or one or one more
        // map: one to one
        @Override
        public void flatMap(WaterSensor value, Collector<String> out) throws Exception {

            if (value.id.equals("sensor_1")) {
                out.collect(value.getId() + " - " + String.valueOf(value.vc));
            } else if (value.id.equals("sensor_2")) {
                // when invoking collect once , the output one
                out.collect(value.getId() + " - " + String.valueOf(value.ts));
                // next invoking collect, then outputs one again
                out.collect(value.getId() + " - " + String.valueOf(value.vc));
            }
        }
    }
}
