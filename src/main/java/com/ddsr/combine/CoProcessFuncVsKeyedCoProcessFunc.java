package com.ddsr.combine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Choosing between these two functions(CoProcessFunction and KeyedCoProcessFunction) typically depends on whether your
 * operation is related to keys (entities, users, sessions, etc.) and whether you need to maintain state or timers for
 * those keys. If your operation is independent of keys, or you need a global view, you use CoProcessFunction. If you
 * need fine-grained control over each key, you go with KeyedCoProcessFunction.
 *
 * @see ConnectKeyByDemo
 * @see ConnectKeyCoProcessDemo
 * @author ddsr, created it at 2024/1/10 22:11
 */
public class CoProcessFuncVsKeyedCoProcessFunc {

    /**
     * <li>It allows you to perform a co-process operation on two DataStreams.</li>
     * <li>It is a low-level operation giving you full control over state and time.</li>
     * <li>It does not require the input streams to be keyed, and thus it doesn't provide access to keyed state or timers.</li>
     * <li>You handle elements from both streams by implementing the processElement1 and processElement2 methods.</li>
     * <li>Useful for cross-stream operations where you do not have a logical key to partition the data, or you want to
     * manage state globally.</li>
     */
    public class MyCoProcessFunction extends CoProcessFunction<String, String, String> {

        @Override
        public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
            // handle element from the first stream
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
            // handle element from the second stream
        }
    }

    /**
     * <li>It is an extension of CoProcessFunction for keyed streams.</li>
     * <li>It allows you to maintain separate state and timers for each key.</li>
     * <li>You can use the OnTimerContext to perform operations triggered by time events.</li>
     * <li>This is particularly useful when you need to perform operations that are specific to a key, such as
     * maintaining a per-user session or aggregating events per key.</li>
     */
    public class MyKeyedCoProcessFunction extends KeyedCoProcessFunction<String, String, String, String> {

        private ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", String.class));
        }

        @Override
        public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
            // handle element from the first stream with keyed state
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
            // handle element from the second stream with keyed state
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // handle timer event
        }
    }
}
