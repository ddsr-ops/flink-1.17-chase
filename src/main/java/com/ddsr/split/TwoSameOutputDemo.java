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
 * @author ddsr, created it at 2024/1/7 21:39
 */
public class TwoSameOutputDemo {
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


        // Alternatively, you can use two OutputTags with the same name to refer to the same side output, but if you do,
        // they must have the same type.
        // The two same output tags using the same name and the same type
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        OutputTag<String> warnTag1 = new OutputTag<>("warn", Types.STRING);

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(WaterSensor::getId)
//                .process(new ProcessFunction<WaterSensor, WaterSensor>() { // Deprecated Use process(KeyedProcessFunction)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) {
                        if (value.getVc() > 10) {
                            ctx.output(warnTag, "vc > 10");
                        }
                        out.collect(value);
                    }
                });

        process.print();
        process.getSideOutput(warnTag).printToErr("warnTag");  // Printed content the same to warnTag1
        process.getSideOutput(warnTag1).printToErr("warnTag1");

        env.execute();


    }
}
