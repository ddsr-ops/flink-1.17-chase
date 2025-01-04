package com.ddsr.udf;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2025/1/3 16:52
 */
public class AccumulatorDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        env.fromElements(1, 2, 3, 4, 5, 6)
                .map(
                        new RichMapFunction<Integer, Integer>() {
                            private final LongCounter counter = new LongCounter();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                getRuntimeContext().addAccumulator("sum", counter);
                            }


                            @Override
                            public Integer map(Integer value) {
                                this.counter.add(value);
                                return value;
                            }
                        }
                )
                .print();

        JobExecutionResult jobExecutionResult = env.execute("Accumulator Example");

        // In a real job, you'd access the accumulator's result after the job has completed
        // For example, through the JobExecutionResult in a batch job
        Object sum = jobExecutionResult.getAccumulatorResult("sum");

        System.out.println(sum);
    }
}
