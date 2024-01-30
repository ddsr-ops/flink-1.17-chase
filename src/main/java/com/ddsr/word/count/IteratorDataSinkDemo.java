package com.ddsr.word.count;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

/**
 * Flink also provides a sink to collect DataStream results for testing and debugging purposes.
 *
 * @author ddsr, created it at 2024/1/30 18:14
 */
public class IteratorDataSinkDemo {

    /**
     * This method sets up a local environment for debugging and performs some transformations on a data stream.
     * It then collects and prints the results.
     */
    public static void main(String[] args) throws Exception {
        // Use local environment for debugging
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5);

        // Perform transformation: multiply each element by 2
        SingleOutputStreamOperator<Integer> multipleByTwoDataStream = integerDataStream.map(i -> i * 2);

        // Collect the transformed data stream asynchronously
        Iterator<Integer> iterator = multipleByTwoDataStream.collectAsync();

        // Execute the environment asynchronously
        env.executeAsync();

        // Print the collected results
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
