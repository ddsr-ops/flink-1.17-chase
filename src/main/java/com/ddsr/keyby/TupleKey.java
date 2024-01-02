package com.ddsr.keyby;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeySelectors aren't limited to extracting a key from your events. They can,
 * instead, compute the key in whatever way you want, as long as the resulting key
 * is deterministic, and has valid implementations of {@code hashCode()} and
 * {@code equals()}. This restriction rules out KeySelectors that generate random
 * numbers, or that return Arrays or Enums. However, you can have composite keys
 * using Tuples or POJOs, for example, as long as their elements follow these same
 * rules.
 * The keys must be produced in a deterministic way, because they are recomputed
 * whenever they are needed, rather than being attached to the stream records.
 */
public class TupleKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);


        ds.map(s -> new Event(s.split(",")[0], s.split(",")[1], Long.parseLong(s.split(",")[2])))
                .keyBy(new EventKeySelector())
                // Count after key by, output the string including key and count
                .map(new RichMapFunction<Event, String>() {
                    private transient ValueState<Integer> state;

                    @Override
                    public void open(Configuration config) {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("event-count", Integer.class));
                    }

                    @Override
                    public String map(Event value) throws Exception {
                        Integer currCount = state.value();
                        currCount = currCount == null ? 1 : currCount + 1;
                        state.update(currCount);
                        return value.username + ", " + value.action + ", " + currCount;
                    }
                })
                .print();


        env.execute();

    }

    private static class EventKeySelector implements KeySelector<Event, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(Event event) {
            return new Tuple2<>(event.username, event.action);
        }
    }

    private static class Event {
        public String username;
        public String action;
        public long timestamp;

        public Event(String username, String action, long timestamp) {
            this.username = username;
            this.action = action;
            this.timestamp = timestamp;
        }
    }
}
