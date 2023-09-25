package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author ddsr, created it at 2023/8/20 17:28
 */
public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 增量聚合Aggregate
        // 1. 属于本窗口的第一条数据来，创建第一个窗口，创建累加器（不来，就不创建）
        // 2. 增量聚合， 数据来一条，调用一次add方法
        // 3. 关窗时，调用getResult方法
        // 4. IN, ACC, OUT类型可不一样，比reduce更加灵活
        SingleOutputStreamOperator<String> aggregate = sensorWS
                .aggregate(
                        // 输入数据类型，中间状态类型，输出数据类型
                        new AggregateFunction<WaterSensor, Integer, String>() {
                            @Override
                            public Integer createAccumulator() {
                                // 累加器的初始值
                                System.out.println("创建累加器");
                                return 0;
                            }

                            // 聚合逻辑
                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                int addedValue = accumulator + value.getVc();
                                System.out.println("调用add方法,中间状态为" + accumulator.toString() + ",新来的value="+value + "add后的值为" + addedValue);
                                return addedValue;
                            }

                            // 获得最终结果，关窗时输出
                            @Override
                            public String getResult(Integer accumulator) {
                                System.out.println("调用getResult方法");
                                return accumulator.toString();
                            }

                            // 只有会话窗口才会用到，很少用它
                            @Override
                            public Integer merge(Integer a, Integer b) {
                                System.out.println("调用merge方法");
                                return null;
                            }
                        }
                );

        aggregate.print();

        env.execute();
    }
}
