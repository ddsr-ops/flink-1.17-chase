package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * A demo for TTL of state
 *
 * s1,1,11  <=== update state, output vc is null
 * s1,1,1   <===  input this in 5 seconds after s1,1,11, output vc is 11
 * s1,2,2   <===  input this in 5 seconds after s1,1,1, output vc is 11
 * s1,3,3   <===  input this in 5 seconds after s1,2,2, output vc is 11
 * s1,4,4   <===  input this in 5 seconds after s1,3,3, output vc is 11
 * s1,5,5   <===  input this at least 5 seconds later after s1,4,4, output vc is null because state is cleared
 *
 * key=s1,状态值=null
 * key=s1,状态值=11
 * key=s1,状态值=11
 * key=s1,状态值=11
 * key=s1,状态值=11
 * key=s1,状态值=null
 *
 *
 * @author ddsr, created it at 2023/9/30 16:49
 */
public class StateTTLDemo {
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
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ValueState<Integer> lastVcState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // 1.创建 StateTtlConfig
                                StateTtlConfig stateTtlConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(5)) // 过期时间5s
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值， 一般来说使用这个选项值
//                                        .disableCleanupInBackground()
                                        // By default, expired values are explicitly removed on read, such as
                                        // ValueState#value, and periodically garbage collected in the background if
                                        // supported by the configured state backend. Background cleanup can be disabled
                                        .build();

                                //  2.状态描述器 启用 TTL
                                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                                stateDescriptor.enableTimeToLive(stateTtlConfig);


                                this.lastVcState = getRuntimeContext().getState(stateDescriptor);

                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 先获取状态值，打印 ==》 读取状态
                                Integer lastVc = lastVcState.value();
                                out.collect("key=" + value.getId() + ",状态值=" + lastVc);

                                // 如果水位大于10，更新状态值 ===》 写入状态
                                if (value.getVc() > 10) {
                                    lastVcState.update(value.getVc());
                                }
                            }
                        }
                )
                .print();

        env.execute();
    }
}
