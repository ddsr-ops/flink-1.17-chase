package com.ddsr.iterations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class IterativeStreamExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Define the output tag for the side output
        final OutputTag<Integer> outputTag = new OutputTag<Integer>("side-output") {};

        // Create a stream of numbers from 0 to 9
        DataStream<Integer> inputStream = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Create an IterativeStream from the input stream
        IterativeStream<Integer> iteration = inputStream.iterate();

        // Define the step function that increments the numbers
        SingleOutputStreamOperator<Integer> incremented = iteration.map(value -> {
            // Increment the value
            return value + 1;
        });

        // Split the stream: numbers less than 10 continue in the loop, others are emitted to the side output
        SingleOutputStreamOperator<Integer> feedback = incremented.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                if (value < 10) {
                    // Continue the iteration
                    out.collect(value);
                } else {
                    // Emit to the side output and exit the iteration loop
                    ctx.output(outputTag, value);
                }
            }
        });

        // Close the iteration by defining the feedback stream
        iteration.closeWith(feedback);

        // Retrieve the side output stream and print its elements
        DataStream<Integer> sideOutputStream = feedback.getSideOutput(outputTag);
        sideOutputStream.print();

        // Execute the Flink job
        env.execute("Iterative Stream Example");
    }
}
