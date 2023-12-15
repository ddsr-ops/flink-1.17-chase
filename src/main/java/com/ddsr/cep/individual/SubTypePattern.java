package com.ddsr.cep.individual;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/14 8:01
 */
public class SubTypePattern {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // test case: input 1,2  2,6  1  3,8
        // output : {start=[SubEvent{id=1, number=2}], next=[SubEvent{id=2, number=6}]}
        //          {start=[Event{id=1}], next=[SubEvent{id=3, number=8}]}
        DataStream<Event> eventStream = ds.map(s -> {
            // If comma in the string, the string will be split into two parts
            if (s.contains(",")) {
                String[] parts = s.split(",");
                int id = Integer.parseInt(parts[0]);
                int number = Integer.parseInt(parts[1]);
                return new SubEvent(id, number);
            }
            // If not, the string will be converted to an integer
            return new Event(Integer.parseInt(s));
        });

        Pattern<Event, SubEvent> pattern = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(s -> s.getId() == 1))
                .next("next")
                .subtype(SubEvent.class)
                .where(SimpleCondition.of(s -> s.getNumber() >= 5));

        PatternStream<Event> patternStream = CEP.pattern(eventStream, pattern).inProcessingTime();

        patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                out.collect(match.toString());
            }
        }).print();


        env.execute();

    }

    public static class Event {
        private final int id;

        public Event(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "id=" + id +
                    '}';
        }
    }

    public static class SubEvent extends Event {
        private final int number;

        public SubEvent(int id, int number) {
            super(id);
            this.number = number;
        }

        public int getNumber() {
            return number;
        }

        @Override
        public String toString() {
            return "SubEvent{" +
                    "id=" + super.getId() +
                    ", number=" + number +
                    '}';
        }
    }
}
