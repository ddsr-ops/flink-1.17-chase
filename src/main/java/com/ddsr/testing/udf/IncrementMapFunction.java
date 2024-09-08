package com.ddsr.testing.udf;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Unit testing stateless, timeless udfs using flink-test-utils. See
 * {@link com.ddsr.testing.udf.IncrementMapFunctionTest}
 *
 * @author ddsr, created it at 2024/9/2 18:28
 */
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
