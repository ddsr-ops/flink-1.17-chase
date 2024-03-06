package com.ddsr.state;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * This keeps a value that can be updated and retrieved (scoped to key of the input element as mentioned above, so there
 * will possibly be one value for each key that the operation sees). The value can be set using update(T) and retrieved
 * using T value().
 *
 * @author ddsr, created it at 2023/9/26 22:37
 */
public class KeyedValueStateDemo {
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
                    // Define a value state, based on the key, which is only initialized in the open method
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // the name of state, must be unique; the type of state
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

//                        lastVcState.value() ; // get the value of lastVcState
//                        lastVcState.update(0); // update the value of lastVcState
//                        lastVcState.clear(); // clear the value of lastVcState

                        // get the value of the state, 0 if null(the default value of Integer class is null)
                        int lastVc = lastVcState.value()  == null ? 0 : lastVcState.value();

                        // if the different value between contiguous data is more than 10, then print
                        Integer vc = value.getVc();
                        if (Math.abs(vc - lastVc) > 10) {
                            out.collect("传感器=" + value.getId() + "==>当前水位值=" + vc + ",与上一条水位值=" + lastVc + ",相差超过10！！！！");
                        }

                        // store the value into the state
                        lastVcState.update(vc);


                    }
                })
                .print();


        env.execute();

    }
}
