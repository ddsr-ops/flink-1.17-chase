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
 * href=https://nightlies.apache.org/flink/flink-docs-release-1
 * .20/docs/dev/datastream/testing/#unit-testing-stateful-or-timely-udfs--custom-operators>Testing</a>
 */
public class StatefulFlatMap extends RichFlatMapFunction<Long, Long> {

    // The transient keyword is used for the following reasons:
    //
    //Serialization Control: Fields marked as transient are excluded from the default serialization process. This is
    // useful for fields that are meant to be temporary or derived, or fields that are not necessary to be persisted.
    //Avoid Serialization Overhead: Some fields, like those involving state management in frameworks like Apache
    // Flink, might not be serializable or might be expensive to serialize. Using transient avoids the overhead of
    // serializing these fields.
    //Framework Specific Needs: In distributed computing frameworks like Apache Flink, state management is often
    // handled by the framework itself, and it uses its own mechanisms to checkpoint and restore state. Marking the
    // state as transient ensures that the framework's state management mechanisms are used rather than Java's
    // default serialization.
    //So, in this case, transient tells Java not to serialize the state field, allowing Flink to manage the state
    // serialization and deserialization process efficiently.
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