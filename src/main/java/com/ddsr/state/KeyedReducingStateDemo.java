package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ReducingState keeps a single value that represents the aggregation of all values added to the state. The interface is
 * similar to ListState but elements added using add(T) are reduced to an aggregate using a specified ReduceFunction.
 * <p>
 * Compute the sum of vc of every sensor
 * The type of reducing states is the same as the type of input data
 *
 * @author ddsr, created it at 2023/9/29 19:44
 */
public class KeyedReducingStateDemo {
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

                    // vcCount is a ReducingState, will be reduced when adding a new value
                    // Reducing function is specified in the ReducingStateDescriptor
                    ReducingState<Integer> vcCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcCount = getRuntimeContext()
                                .getReducingState(
                                        new ReducingStateDescriptor<>("vcCount",
                                                (ReduceFunction<Integer>) Integer::sum, Types.INT)
                                );
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                        vcCount.add(value.getVc());
                        out.collect(value.getId() + " vc = " + vcCount.get());

                        // Returns the current value for the state. When the state is not partitioned the returned value is the same for all inputs in a given operator instance. If state partitioning is applied, the value returned depends on the current operator input, as the operator maintains an independent state for each partition.
//                        vcCount.get();
//                        vcCount.clear();
                    }
                })
                .print();

        env.execute();


    }
}
