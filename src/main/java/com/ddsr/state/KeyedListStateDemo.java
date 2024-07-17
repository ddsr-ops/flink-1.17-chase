package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ListState keeps a list of elements. You can append elements and retrieve an Iterable over all currently stored
 * elements.
 * Elements are added using add(T) or addAll(List<T>), the Iterable can be retrieved using Iterable<T> get(). You can
 * also override the existing list with update(List<T>)
 * <p>
 * Obtain top 3 vc of every sensor
 *
 * @author ddsr, created it at 2023/9/28 22:41
 */
public class KeyedListStateDemo {
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

                            /*
                             * Flink provides MapState and ListState types that are optimized for RocksDB. Where
                             * possible, these should be used instead of a ValueState object holding some sort of
                             * collection. The RocksDB state backend can append to ListState without going through
                             * (de)serialization, and for MapState, each key/value pair is a separate RocksDB object,
                             *  so MapState can be efficiently accessed and updated.
                             */
                            ListState<Integer> vcListState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<>("vcListState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                /*vcListState.add(value.getVc());
                                Iterable<Integer> vcIterable = vcListState.get();
                                List<Integer> vcListForSort = new ArrayList<>();
                                vcIterable.forEach(vcListForSort::add);
                                // descend sort
                                vcListForSort.sort((o1, o2) -> o2 - o1);
                                if (vcListForSort.size() > 3) {
                                    vcListForSort.remove(3);
                                }
                                out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcListForSort);
                                vcListState.update(vcListForSort);*/


                                // 1.来一条，存到list状态里
                                vcListState.add(value.getVc());

                                // 2.从list状态拿出来(Iterable)， 拷贝到一个List中，排序， 只留3个最大的
                                Iterable<Integer> vcListIt = vcListState.get();
                                // 2.1 拷贝到List中
                                List<Integer> vcList = new ArrayList<>();
                                for (Integer vc : vcListIt) {
                                    vcList.add(vc);
                                }
                                // 2.2 对List进行降序排序
//                                vcList.sort((o1, o2) -> o2 - o1);
                                vcList.sort(Collections.reverseOrder());
                                // 2.3 只保留最大的3个(list中的个数一定是连续变大，一超过3就立即清理即可)
                                if (vcList.size() > 3) {
                                    // 将最后一个元素清除（第4个）
//                                    vcList.remove(3);
                                    vcList = vcList.subList(0, 3); // more efficient comparing with the above
                                }

                                out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcList);

                                // 3.更新list状态
                                vcListState.update(vcList);


//                                vcListState.get();            //取出 list状态 本组的数据，是一个Iterable
//                                vcListState.add();            // 向 list状态 本组 添加一个元素, no affection for other key
//                                vcListState.addAll();         // 向 list状态 本组 添加多个元素
//                                vcListState.update();         // 更新 list状态 本组数据（覆盖）
//                                vcListState.clear();          // 清空List状态 本组数据
                            }
                        }
                )
                .print();

        env.execute();
    }
}
