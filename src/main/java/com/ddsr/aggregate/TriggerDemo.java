package com.ddsr.aggregate;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author ddsr, created it at 2023/10/19 22:09
 */
public class TriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_1", 2L, 3),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //TODO: implement the MyTrigger, giving an example is ok
        stream.keyBy(e -> e.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .trigger(new Trigger() {
                            @Override
                            public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
                                return null;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                                return null;
                            }

                            @Override
                            public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                                return null;
                            }

                            @Override
                            public void clear(Window window, TriggerContext ctx) throws Exception {

                            }
                        });

        env.execute();


    }
}
