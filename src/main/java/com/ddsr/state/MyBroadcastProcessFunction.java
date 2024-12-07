package com.ddsr.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2024/12/7 19:23
 */
public class MyBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, String, String, String> {
    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // process elements from the keyed stream
        // NO applyToKeyedState method
    }

    @Override
    public void processBroadcastElement(String value,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        // define the state descriptor for the keyed state you want to manipulate
        MapStateDescriptor<String, Integer> keyedStateDescriptor = new MapStateDescriptor<>("keyed-state",
                StringSerializer.INSTANCE, IntSerializer.INSTANCE); // <key, value>

        ctx.applyToKeyedState(keyedStateDescriptor,
                new KeyedStateFunction<String, MapState<String, Integer>>() {
                    @Override
                    public void process(String key, MapState<String, Integer> state) throws Exception {
                      // custom logic to apply to all states of all keys
                        if (state.contains(key)) {
                            // if key exists, increment counter
                            Integer currentVal = state.get(key);
                            state.put(key, currentVal + 1);
                        } else {
                            // If key doesn't exist, set counter to 1 to start
                            state.put(key, 1);
                        }
                    }
                });
    }
}
