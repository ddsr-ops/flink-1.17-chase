package com.ddsr.tranforms;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Evaluates a boolean function for each element and retains those for which the function returns true.
 *
 * @author ddsr, created it at 2023/8/18 21:47
 */
public class TransFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(

                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        // 方式一：传入匿名类实现FilterFunction
//        stream.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor e) throws Exception {
//                return e.id.equals("sensor_1");
//            }
//        }).print();

//         方式二：传入FilterFunction实现类
         stream.filter(new UserFilterById("sensor_1")).print();

        env.execute();
    }
    public static class UserFilter implements FilterFunction<WaterSensor> {
        @Override
        public boolean filter(WaterSensor e) throws Exception {
            return e.id.equals("sensor_1");
        }
    }

    public static class UserFilterById implements FilterFunction<WaterSensor> {
        public String id;

        public UserFilterById(String id) {
            this.id = id;
        }

        @Override
        public boolean filter(WaterSensor e) throws Exception {
            return id.equals(e.getId());
        }
    }
}
