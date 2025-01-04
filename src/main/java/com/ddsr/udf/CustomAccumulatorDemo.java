package com.ddsr.udf;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A demo using custom accumulator
 *
 * @author ddsr, created it at 2025/1/4 19:24
 */
public class CustomAccumulatorDemo {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        env.fromElements("hello",
                        "world",
                        "flink",
                        "tft",
                        "china"
                )
                .map(new RichMapFunction<String, String>() {
                    private transient StringConcatenator concatAccumulator;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        concatAccumulator = new StringConcatenator(", ");
                        getRuntimeContext().addAccumulator("stringConcat", concatAccumulator);
                    }

                    @Override
                    public String map(String value) {
                        concatAccumulator.add(value);
                        return value;
                    }
                })
                .print();

        JobExecutionResult jobExecutionResult = env.execute("custom accumulator demo");

        Object concatString = jobExecutionResult.getAccumulatorResult("stringConcat");
        System.out.println(concatString);

    }

}
