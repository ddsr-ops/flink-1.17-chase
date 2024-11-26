package com.ddsr.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

/**
 * The map state with TTL currently supports null user values only if the user value serializer can handle null values.
 * If the serializer does not support null values, it can be wrapped with NullableSerializer at the cost of an extra
 * byte in the serialized form
 * <p>
 *
 * org.apache.flink.types.NullFieldException: Field 1 is null, but expected to hold a value.
 *
 * @author ddsr, created it at 2024/11/25 18:10
 */
public class NullInMapStateDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sample data stream using a custom source function
        DataStream<Tuple2<String, Integer>> input = env.addSource(new CustomSourceFunction());

        // Apply a transformation with stateful processing
        DataStream<String> result = input
                .keyBy(tuple -> tuple.f0)
                .process(new StatefulMapFunction());

        // Print the result
        result.print();

        // Execute the job
        env.execute("Flink Map State with TTL Example");

    }

    // Custom source function to emit elements, including null values
    public static class CustomSourceFunction extends RichSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            ctx.collect(Tuple2.of("key1", 1));
            ctx.collect(Tuple2.of("key2", 2));
            ctx.collect(Tuple2.of("key1", null)); // Null value example
            ctx.collect(Tuple2.of("key2", 3));
            isRunning = false;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class StatefulMapFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {

        private transient MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Configure TTL
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            // Create a MapStateDescriptor with NullableSerializer
            MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                    "mapState",
                    StringSerializer.INSTANCE,
                    NullableSerializer.wrapIfNullIsNotSupported(IntSerializer.INSTANCE, false) // Wrap the serializer
                    // to handle null values
            );

            mapStateDescriptor.enableTimeToLive(ttlConfig);

            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String,
                Integer>, String>.Context ctx, Collector<String> out) throws Exception {
            // Update the state
            mapState.put(value.f0, value.f1);

            // Collect the current state for demonstration purposes
            out.collect("State for key: " + value.f0 + " is " + mapState.get(value.f0));

        }
    }
}
