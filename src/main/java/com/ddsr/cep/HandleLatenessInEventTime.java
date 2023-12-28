package com.ddsr.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * In CEP (Complex Event Processing), the order of element processing is crucial. To ensure elements
 * are processed in the correct order when working in event time, an incoming element is initially
 * placed into a buffer. Elements in this buffer are sorted in ascending order by their timestamp.
 * When a watermark arrives, all elements in the buffer with timestamps smaller than the watermark's
 * timestamp are processed. This ensures that elements between watermarks are processed in event-time
 * order.
 *
 * <p>Note: The library assumes the correctness of the watermark when working in event time. </p>
 *
 * To guarantee that elements across watermarks are processed in event-time order, Flink's CEP library assumes
 * the correctness of the watermark. Elements with a timestamp smaller than that of the last seen
 * watermark are considered late and are not processed further. Additionally, a sideOutput tag can be
 * specified to collect late elements that arrive after the last seen watermark. Usage is as follows:
 *
 * <pre>{@code
 * // Example code to specify a sideOutput tag for late elements
 * OutputTag<T> sideOutputTag = new OutputTag<T>("side-output") {};
 * }</pre>
 */
public class HandleLatenessInEventTime {

    private static class Event {
        // timestamp as event time
        long ts;
        double temperature;

        public Event(long l, double v) {
            ts = l;
            temperature = v;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "ts=" + ts +
                    ", temperature=" + temperature +
                    '}';
        }
    }

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // map ds to event stream
        DataStream<Event> input = ds.map(s -> {
            String[] tokens = s.split(",");
            return new Event(Long.parseLong(tokens[0]), Double.parseDouble(tokens[1]));
        });

        // define an out-of-orderness watermark strategy, extracting field ts of event as watermark
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((event, timestamp) -> event.ts);

        // Assign the watermark strategy to the input
        input.assignTimestampsAndWatermarks(watermarkStrategy);

        // Define a pattern that matches if the consecutive two temperatures are greater than 20
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) {
                        return value.temperature > 20;
                    }
                })
                .next("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) {
                        return value.temperature > 20;
                    }
                })
                .within(Time.seconds(5));

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        OutputTag<Event> lateDataOutputTag = new OutputTag<Event>("late-data"){};

        SingleOutputStreamOperator<Event> result = patternStream
                .sideOutputLateData(lateDataOutputTag)
                .select(
                        new PatternSelectFunction<Event, ComplexEvent>() {...}
                );

        DataStream<Event> lateData = result.getSideOutput(lateDataOutputTag);

    }
}
