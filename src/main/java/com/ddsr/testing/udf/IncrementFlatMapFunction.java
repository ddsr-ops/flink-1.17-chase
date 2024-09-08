package com.ddsr.testing.udf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2024/9/9 6:45
 */
public class IncrementFlatMapFunction implements FlatMapFunction<Long, Long> {


    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        out.collect(value + 1);
    }
}
