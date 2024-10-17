package com.ddsr.state;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Unit testing stateful udfs using flink-test-utils
 *
 * @author ddsr, created it at 2024/10/17 18:09
 * @see <a
 * href=https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/testing/#unit-testing-stateful-or-timely-udfs--custom-operators>Testing</a>
 */
public class StatefulFlatMap extends RichFlatMapFunction<Long, Long> {

    private transient ValueState<Long> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "sumState", // the state name
                Types.LONG // type information
        );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        Long currentState = state.value();
        if (currentState == null) {
            currentState = 0L;
        }
        currentState += value;
        state.update(currentState);
        out.collect(currentState);
    }
}