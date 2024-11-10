package com.ddsr.checkpoint;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * One obvious outlier is when you want to use a bounded job to bootstrap some job state that you then want to use in an
 * unbounded job. For example, by running a bounded job using STREAMING mode, taking a savepoint, and then restoring
 * that savepoint on an unbounded job.
 *
 * @see UnBoundedJob
 */
@SuppressWarnings({"deprecation", "DuplicatedCode"})
public class BoundedJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-group");
        props.setProperty("max.poll.records", "10"); // Read only 1000 records meaning a batch


        // Kafka message likes this: 1 2 3 4 5 6 7 8 9 10
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("bootstrap_topic", new SimpleStringSchema(),
                props);
        env.addSource(consumer)
                .map((MapFunction<String, Integer>) Integer::parseInt)
                .map(new RichMapFunction<Integer, Integer>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<>("state", TypeInformation.of(Integer.class));
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        Integer currentState = state.value();
                        if (currentState != null) {
                            System.out.println("Current state: " + currentState);
                            // Update the state
                            state.update(currentState + value);
                            return currentState + value;
                        } else {
                            // Initialize the state if it's null
                            state.update(value);
                            return value;
                        }
                    }
                })
                .print();

        env.execute("Bounded Job");
    }
}