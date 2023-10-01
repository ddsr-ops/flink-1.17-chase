package com.ddsr.checkpoint;

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
 * @author ddsr, created it at 2023/10/1 22:46
 */
public class CheckpointConfigDemo {
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

/**
 *
 * 1、 Barrier对齐： 一个Task收到所有上游的任务的同一个编号的Barrier，才会对自己本地的状态进行备份
 *        精准一次：在Barrier对齐过程中，Barrier后面的数据阻塞等待（不会越过Barrier）
 *        至少一次：在Barrier对齐过程中，先到的Barrier不会阻塞其后的数据，接着计算
 *
 * 2、 非Barrier对齐： 一个Task在收到第一个Barrier时就开始执行备份，能保证精准一次（Flink1.11 later）
 *        先到的Barrier（Task的输入缓存区）， 此时开始执行本地状态备份，不阻塞其后面的数据，继续计算输出， 越过数据（输入缓冲，输出缓冲）
 *                  ，Barrier被放置Task的输出缓存区末尾（最先传递出去），同时被越过的数据被持久化到状态中
 *        未到的Barrier（未到Task的输入缓存区，可能在上游Task输出缓冲中），在它前面的数据继续计算输出，同时这部分数据被持久化到状态中
 *        最后一个分区Barrier到达时，该Task的状态备份完成
 *
 *
 */