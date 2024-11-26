package com.ddsr.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * The map state with TTL currently supports null user values only if the user value serializer can handle null values.
 * If the serializer does not support null values, it can be wrapped with NullableSerializer at the cost of an extra
 * byte in the serialized form
 *
 * @author ddsr, created it at 2024/11/25 18:10
 */
public class NullInMapStateDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define custom TypeInformation for the input data
        TypeInformation<Tuple2<String, Integer>> typeInfo = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        });

        // Sample data stream with custom TypeInformation
        DataStreamSource<Tuple2<String, Integer>> input = env.fromCollection(
                Arrays.asList(
                        Tuple2.of("key1", 1),
                        Tuple2.of("key2", 2),
                        Tuple2.of("key1", null), // Null value example
                        Tuple2.of("key2", 3)),
                typeInfo);

        // Apply a transformation with stateful processing
        DataStream<String> result = input
                .keyBy(tuple -> tuple.f0)
                .process(new StatefulMapFunction());

        // Print the result
        result.print();

        // Execute the job
        env.execute("Flink Map State with TTL Example");

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
