package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * AggregatingState keeps a single value that represents the aggregation of all values added to the state. Contrary
 * to ReducingState, the aggregate type may be different from the type of elements that are added to the state. The
 * interface is the same as for ListState but elements added using add(IN) are aggregated using a specified
 * AggregateFunction.
 *
 * <p></p>
 * Compute the average of vc of every sensor
 *
 * The largest difference is types of IN, ACC, OUT can be different
 *
 * <p><IN> – The type of the values that are aggregated (input values)
 * <p><ACC> – The type of the accumulator (intermediate aggregate state).
 * <p><OUT> – The type of the aggregated result
 *
 * @author ddsr, created it at 2023/9/29 20:13
 */
public class KeyedAggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    // the first type parameter is sum of vc
                    // the second type parameter is average of the sensor
                    AggregatingState<Integer, Double> avgVcAggState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        avgVcAggState = getRuntimeContext()
                                .getAggregatingState(
                                        new AggregatingStateDescriptor<>(
                                                "avgVcAggState",
                                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                    @Override
                                                    public Tuple2<Integer, Integer> createAccumulator() {
                                                        return Tuple2.of(0, 0);
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                        // f0 is sum of vc
                                                        // f1 is count
                                                        return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                                    }

                                                    @Override
                                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                        // divide f0 by f1,which is average
                                                        return accumulator.f0 * 1D / accumulator.f1;
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                        return null;
                                                    }
                                                },
                                                Types.TUPLE(Types.INT, Types.INT)
                                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        // add vc to avgVcAggState
                        avgVcAggState.add(value.getVc());

                        out.collect("传感器id为" + value.getId() + ",平均水位值=" + avgVcAggState.get());

//                                vcAvgAggregatingState.get();    // 对 本组的聚合状态 获取结果
//                                vcAvgAggregatingState.add();    // 对 本组的聚合状态 添加数据，会自动进行聚合
//                                vcAvgAggregatingState.clear();  // 对 本组的聚合状态 清空数据
                    }
                })
                .print();

        env.execute();
    }
}
