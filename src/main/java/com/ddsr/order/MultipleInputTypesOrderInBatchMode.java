package com.ddsr.order;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * In BATCH execution mode, there are some operations where Flink guarantees order. The ordering can be a side effect of
 * the particular task scheduling, network shuffle, and state backend (see above), or a conscious choice by the system.
 * <p>
 * There are three general types of input that we can differentiate:
 * <p>
 * broadcast input: input from a broadcast stream (see also Broadcast State)
 * regular input: input that is neither broadcast nor keyed
 * keyed input: input from a KeyedStream
 * Functions, or Operators, that consume multiple input types will process them in the following order:
 * <p>
 * broadcast inputs are processed first
 * regular inputs are processed second
 * keyed inputs are processed last
 * For functions that consume from multiple regular or broadcast inputs — such as a CoProcessFunction — Flink has the
 * right to process data from any input of that type in any order.
 * <p>
 * For functions that consume from multiple keyed inputs — such as a KeyedCoProcessFunction — Flink processes all
 * records for a single key from all keyed inputs before moving on to the next.
 *
 * @author ddsr, created it at 2024/11/12 18:12
 */
public class MultipleInputTypesOrderInBatchMode {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a regular input stream
        DataStream<String> regularStream = env.fromElements("regular-1", "regular-2");

        // Create a keyed input stream
        KeyedStream<String, String> keyedStream = env.fromElements("keyed-1", "keyed-2")
                .keyBy(value -> value);

        // Create a broadcast input stream
        DataStream<String> broadcastStream = env.fromElements("broadcast-1", "broadcast-2");
        MapStateDescriptor<String, String> broadcastStateDescriptor = new MapStateDescriptor<>(
                "broadcastState",
                String.class,
                String.class
        );
        BroadcastStream<String> broadcastInput = broadcastStream.broadcast(broadcastStateDescriptor);

        // Connect the broadcast stream with the regular stream
        BroadcastConnectedStream<String, String> connectedStreams = regularStream.connect(broadcastInput);

        // Process the connected streams
        DataStream<String> processedStream = connectedStreams.process(
                new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value,
                                               BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx,
                                               Collector<String> out) {

                        System.out.println(value);
                    }

                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<String, String,
                            String>.Context ctx, Collector<String> out) {
                        System.out.println(value);

                    }
                }
        );

        // Connect the processed stream with the keyed stream
        ConnectedStreams<String, String> finalConnectedStreams = processedStream.connect(keyedStream);

        // Process the final connected streams
        finalConnectedStreams
                .map(new CoMapFunction<String, String, String>() {
                    @Override
                    public String map1(String value) {
                        System.out.println(value);
                        return "Final processed stream: " + value;
                    }

                    @Override
                    public String map2(String value) {
                        System.out.println(value);
                        return "Processed keyed input: " + value;
                    }
                })
                .print();

        env.execute("Flink Input Order Example");
    }
}
