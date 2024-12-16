package com.ddsr.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Now that you have activated queryable state on your cluster, it is time to see how to use it. In order for a state to
 * be visible to the outside world, it needs to be explicitly made queryable by using:
 * <p>
 * either a QueryableStateStream, a convenience object which acts as a sink and offers its incoming values as
 * queryable state, or the stateDescriptor.setQueryable(String queryableStateName) method, which makes the keyed
 * state represented by the state descriptor, queryable. The following sections explain the use of these two approaches.
 *
 * @author ddsr, created it at 2024/12/14 20:58
 */
public class QueryableStateDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("stateName", String.class);

        DataStreamSource<String> stream = env.fromElements("1", "2", "3");

        KeyedStream<String, String> keyedStream = stream.keyBy(e -> e);
        QueryableStateStream<String, String> queryableState = keyedStream.asQueryableState("value-state",
                valueStateDescriptor);

        QueryableStateStream<String, String> queryableState1 = keyedStream.asQueryableState("value-state1");

        // for reducing state
        QueryableStateStream<String, String> queryableState2 = keyedStream.asQueryableState("reduce-state",
                new ReducingStateDescriptor<>(
                "reduce-state",
                        (ReduceFunction<String>) (value1, value2) -> value1 + "+" + value2,
                String.class
        ));

        // The returned QueryableStateStream can be seen as a sink and cannot be further transformed.
        System.out.println(queryableState.getStateDescriptor().isQueryable());

        System.out.println(queryableState1.getStateDescriptor().getQueryableStateName());

        System.out.println(queryableState2.getStateDescriptor().isQueryable());

        env.execute();
    }
}
