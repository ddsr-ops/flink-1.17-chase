package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
                            ListState<Integer> vcListState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                // 1.创建 StateTtlConfig
                                StateTtlConfig stateTtlConfig = StateTtlConfig
                                        .newBuilder(Time.seconds(5)) // 过期时间5s
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期的状态值， 一般来说使用这个选项值
                                        // By default, expired values are explicitly removed on read, such as
                                        // ValueState#value, and periodically garbage collected in the background if
                                        // supported by the configured state backend. Background cleanup can be disabled
//                                        .disableCleanupInBackground()
                                        // Activate the cleanup at the moment of taking the full state snapshot which
                                        // will reduce its size
                                        // This option is not applicable for the incremental checkpointing in the RocksDB
                                        // state backend
//                                        .cleanupFullSnapshot()
                                        // For existing jobs, this cleanup strategy can be activated or deactivated
                                        // anytime in StateTtlConfig, e.g. after restart from savepoint.

                                        // Another option is to trigger cleanup of some state entries incrementally.
                                        // The trigger can be a callback from each state access or/and each record
                                        // processing. If this cleanup strategy is active for certain state, The
                                        // storage backend keeps a lazy global iterator for this state over all its
                                        // entries. Every time incremental cleanup is triggered, the iterator is
                                        // advanced. The traversed state entries are checked and expired ones are
                                        // cleaned up.
                                        // This strategy has two parameters. The first one is number of checked state
                                        // entries per each cleanup triggering. It is always triggered per each state
                                        // access. The second parameter defines whether to trigger cleanup
                                        // additionally per each record processing. The default background cleanup
                                        // for heap backend checks 5 entries without cleanup per record processing.
                                        // // // Incremental cleanup is Only for Heap state backend, not for RocksDB
                                        // Note:
                                        //     1. If no access happens to the state or no records are processed, expired state will persist.
                                        //     2. Time spent for the incremental cleanup increases record processing latency.
//                                        .cleanupIncrementally(10, true)
                                        // For existing jobs, this cleanup strategy can be activated or deactivated
                                        // anytime in StateTtlConfig, e.g. after restart from savepoint.

                                        // If the RocksDB state backend is used, a Flink specific compaction filter
                                        // will be called for the background cleanup. RocksDB periodically runs
                                        // asynchronous compactions to merge state updates and reduce storage. Flink
                                        // compaction filter checks expiration timestamp of state entries with TTL
                                        // and excludes expired values.
                                        // Calling of TTL filter during compaction slows it down. The TTL filter has
                                        // to parse timestamp of last access and check its expiration for every
                                        // stored state entry per key which is being compacted. In case of collection
                                        // state type (list or map) the check is also invoked per stored element.
                                        // If this feature is used with a list state which has elements with
                                        // non-fixed byte length, the native TTL filter has to call additionally a
                                        // Flink java type serializer of the element over JNI per each state entry
                                        // where at least the first element has expired to determine the offset of
                                        // the next unexpired element.
                                        // In Flink 1.20, periodic compaction time can be configured, but the 1.17
                                        // can`t. Through cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries,
                                        // Duration periodicCompactionTime) method
//                                        .cleanupInRocksdbCompactFilter(1000)
                                        // For existing jobs, this cleanup strategy can be activated or deactivated
                                        // anytime in StateTtlConfig, e.g. after restart from savepoint.
                                        // Periodic compaction could only work when TTL is enabled.
                                        // Only TTLs in reference to processing time are currently supported.
                                        // This processing time is not relevant to env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                                        .build();

                                //  2.状态描述器 启用 TTL
                                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                                stateDescriptor.enableTimeToLive(stateTtlConfig);
                                this.lastVcState = getRuntimeContext().getState(stateDescriptor);


                                // All state collection types support per-entry TTLs. This means that list elements and map entries expire independently.
                                StateTtlConfig stateTtlConfig1 = StateTtlConfig
                                        .newBuilder(Time.seconds(3)) // 过期时间5s
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 状态 创建和写入（更新） 更新 过期时间
                                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) // 状态 读取、创建和写入（更新） 更新 过期时间
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();
                                ListStateDescriptor<Integer> vcListState1 = new ListStateDescriptor<>("vcListState",
                                        Types.INT);
                                vcListState1.enableTimeToLive(stateTtlConfig1);
                                vcListState = getRuntimeContext().getListState(vcListState1);



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
