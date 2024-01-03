package com.ddsr.combine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * In this example, a control stream is used to specify words which must be filtered out of the streamOfWords. A RichCoFlatMapFunction called ControlFunction is applied to the connected streams to get this done.
 * <p>
 * Note that the two streams being connected must be keyed in compatible ways. The role of a keyBy is to partition a stream’s data, and when keyed streams are connected, they must be partitioned in the same way. This ensures that all of the events from both streams with the same key are sent to the same instance. This makes it possible, then, to join the two streams on that key, for example.
 * <p>
 * In this case the streams are both of type DataStream<String>, and both streams are keyed by the string. As you will see below, this RichCoFlatMapFunction is storing a Boolean value in keyed state, and this Boolean is shared by the two streams.
 * <p>
 * A RichCoFlatMapFunction is a kind of FlatMapFunction that can be applied to a pair of connected streams, and it has access to the rich function interface. This means that it can be made stateful.
 * <p>
 * The blocked Boolean is being used to remember the keys (words, in this case) that have been mentioned on the control stream, and those words are being filtered out of the streamOfWords stream. This is keyed state, and it is shared between the two streams, which is why the two streams have to share the same keyspace.
 * <p>
 * flatMap1 and flatMap2 are called by the Flink runtime with elements from each of the two connected streams – in our case, elements from the control stream are passed into flatMap1, and elements from streamOfWords are passed into flatMap2. This was determined by the order in which the two streams are connected with control.connect(streamOfWords).
 * <p>
 * It is important to recognize that you have no control over the order in which the flatMap1 and flatMap2 callbacks are called. These two input streams are racing against each other, and the Flink runtime will do what it wants to regarding consuming events from one stream or the other. In cases where timing and/or ordering matter, you may find it necessary to buffer events in managed Flink state until your application is ready to process them. (Note: if you are truly desperate, it is possible to exert some limited control over the order in which a two-input operator consumes its inputs by using a custom Operator that implements the InputSelectable
 *
 * @author ddsr, created it at 2024/1/3 22:29
 */
public class ConnectKeyByDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Essential to filter out elements from the control stream
//        env.setParallelism(1);

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }
    }
}
