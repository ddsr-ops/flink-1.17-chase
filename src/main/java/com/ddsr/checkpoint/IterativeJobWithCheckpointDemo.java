package com.ddsr.checkpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an
 * iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a
 * special flag when enabling checkpointing: env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force =
 * true).
 * <p>
 * Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during
 * failure.
 *
 * @author ddsr, created it at 2024/12/13 16:57
 */
public class IterativeJobWithCheckpointDemo {
    // Define the threshold
    private static final int THRESHOLD = 10;

    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // Enable checkpointing with a special flag for iterative jobs
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE, true);

        DataStreamSource<Integer> initialStream = env.fromElements(1, 2, 3, 4, 5);

        // Define the iterative stream
        IterativeStream<Integer> iteration = initialStream.iterate();

        // Define the loop body
        DataStream<Integer> iterationBody = iteration.map(
                (MapFunction<Integer, Integer>) value -> {
                    return value + 1; // Increment the value
                }
        );

        // Feedback stream
        DataStream<Integer> feedback = iterationBody.filter(
                (FilterFunction<Integer>) value -> value <= THRESHOLD
        );

        // Output stream
        DataStream<Integer> output = iterationBody.filter(
                (FilterFunction<Integer>) value -> value > THRESHOLD
        );

        // Close the iteration with the feedback stream ?
        iteration.closeWith(feedback);

        output.print();

        env.execute();
    }
}
