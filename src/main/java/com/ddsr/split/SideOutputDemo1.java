package com.ddsr.split;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author ddsr, created it at 2023/9/24 10:54
 */
public class SideOutputDemo1 {
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


        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(WaterSensor::getId)
//                .process(new ProcessFunction<WaterSensor, WaterSensor>() { // Deprecated Use process(KeyedProcessFunction)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) {
                        if (value.getVc() > 10) {
                            ctx.output(warnTag, "vc > 10");
                        } else {
                            out.collect(value);
                        }
                    }
                });

        process.print();
        process.getSideOutput(warnTag).printToErr();

        env.execute();


    }
}
