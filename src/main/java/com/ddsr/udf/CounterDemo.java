package com.ddsr.udf;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2025/1/3 16:59
 */
public class CounterDemo {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        env.fromElements("apple", "banana", "cherry", "date", "elderberry", "fig", "ship")
                .map(
                        new RichMapFunction<String, String>() {
                            // count the elements
                            private final IntCounter counter = new IntCounter();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                getRuntimeContext().addAccumulator("count", counter);
                            }

                            @Override
                            public String map(String value) {
                                // increment the counter if the element is coming
                                this.counter.add(1);
                                return value;
                            }
                        }
                )
                .print();

        JobExecutionResult jobExecutionResult = env.execute("Counter Demo");

        // Accessing the counter would be done after the job completed, similar to the accumulator
        // For example, through the JobExecutionResult in a batch job
        Object count = jobExecutionResult.getAccumulatorResult("count");

        System.out.println(count);
    }
}
