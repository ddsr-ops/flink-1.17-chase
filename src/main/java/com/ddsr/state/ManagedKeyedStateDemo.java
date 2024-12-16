package com.ddsr.state;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * It is different from QueryableStateDemo{@link QueryableStateDemo} which only supports value state and reducing state.
 * It has no limitations as to which type of state can be made queryable. This means that this can be used for any
 * ValueState, ReduceState, ListState, MapState, and AggregatingState.
 *
 * @author ddsr, created it at 2024/12/16 18:06
 */
public class ManagedKeyedStateDemo {
    public static void main(String[] args) {

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })); // type information
        descriptor.setQueryable("query-name"); // queryable state name


        // note: list states consume much memory
        ListStateDescriptor<String> stringListStateDescriptor = new ListStateDescriptor<>("list-state",
                TypeInformation.of(String.class));

        stringListStateDescriptor.setQueryable("string-list-state");
    }
}
