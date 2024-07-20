package com.ddsr.split;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ddsr, created it at 2023/8/19 20:04
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("192.168.20.126", 7777)
                .map(new WaterSensorMapFunc());

        // 注意types的包， 不要导错org.apache.flink.api.common.typeinfo
        // 泛型 - 输出的类型， id - Tag标志
        OutputTag<WaterSensor> s1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class)){};

        // Reference the same tag with both the same id and the same type
        OutputTag<WaterSensor> s11 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));

        //返回的都是主流
        SingleOutputStreamOperator<WaterSensor> ds1 = ds.process(new ProcessFunction<WaterSensor, WaterSensor>()
        {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                if ("s1".equals(value.getId())) {
                    // tag, data to side-output
                    ctx.output(s1, value);
                } else if ("s2".equals(value.getId())) {
                    ctx.output(s2, value);
                } else {
                    //主流
                    out.collect(value);
                }

            }
        });

        ds1.print("主流，非s1,s2的传感器");
        SideOutputDataStream<WaterSensor> s1DS = ds1.getSideOutput(s1);
        SideOutputDataStream<WaterSensor> s2DS = ds1.getSideOutput(s2);

        // you can use two OutputTags with the same name to refer to the same side output, but if you do, they must have the same type.
        SideOutputDataStream<WaterSensor> s11DS = ds1.getSideOutput(s11);

        s1DS.printToErr("s1"); // s1 will output the same side output as s11
        s2DS.printToErr("s2");
        s11DS.printToErr("s11"); // s11 will output the same side output as s1

        /*
         * s1:11> WaterSensor{id='s1', ts=1, vc=1}
         * s11:11> WaterSensor{id='s1', ts=1, vc=1}
         * 主流，非s1,s2的传感器:12> WaterSensor{id='s3', ts=3, vc=3}
         * s2:1> WaterSensor{id='s2', ts=2, vc=2}
         * s1:2> WaterSensor{id='s1', ts=11, vc=11}
         * s11:2> WaterSensor{id='s1', ts=11, vc=11}
         */


        env.execute();

    }
}
