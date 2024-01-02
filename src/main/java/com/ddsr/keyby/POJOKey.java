package com.ddsr.keyby;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
 * 
 * @see TupleKey
 * @author ddsr, created it at 2024/1/2 21:46
 */
public class POJOKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);


        ds.map(s -> new Event(s.split(",")[0], s.split(",")[1], Long.parseLong(s.split(",")[2])))
                .keyBy(new POJOKeySelector())
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

    // KeySelector that creates a UserActionKey from an Event
    private static class POJOKeySelector implements KeySelector<Event, UserActionKey> {
        @Override
        public UserActionKey getKey(Event event) {
            return new UserActionKey(event.username, event.action);
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
    static class UserActionKey {
        public String username;
        public String action;

        public UserActionKey(String username, String action) {
            this.username = username;
            this.action = action;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserActionKey that = (UserActionKey) o;
            return username.equals(that.username) && action.equals(that.action);
        }

        @Override
        public int hashCode() {
            return 31 * username.hashCode() + action.hashCode();
        }
    }
}
