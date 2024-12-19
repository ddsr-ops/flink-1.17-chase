package com.ddsr.value.types;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

/**
 * Using mutable Value types like IntValue, DoubleValue, etc., can help reduce garbage collection pressure by reusing
 * objects. This is especially beneficial in high-throughput streaming applications where object creation and garbage
 * collection can become bottlenecks.
 *
 * @author ddsr, created it at 2024/12/19 9:38
 */
public class IntValueReusableProcessFunction extends ProcessFunction<Integer, IntValue> { // in, out
    private final IntValue resuableIntValue = new IntValue();

    @Override
    public void processElement(Integer input, Context ctx, Collector<IntValue> out) throws Exception {
        resuableIntValue.setValue(input);
        out.collect(resuableIntValue);
    }
}
