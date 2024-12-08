package com.ddsr.state.examples.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * we have a stream of objects of different colors and shapes and we want to find pairs of objects of the same color
 * that follow a certain pattern, e.g. a rectangle followed by a triangle. We assume that the set of interesting
 * patterns evolves over time.
 *
 * @author ddsr, created it at 2024/12/8 16:01
 * @see <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/broadcast_state/"
 * >broadcast_state</a>
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        // itemSteam consists of items of different colors and shapes
        SingleOutputStreamOperator<Item> itemStream = env.socketTextStream("192.168.20.126", 7777)
                .map(r -> {
                    String[] fields = r.split(",");
                    return new Item(Color.valueOf(fields[2]), Shape.valueOf(fields[1]));
                });

        // ruleStream contains pairs of shapes that are interesting
        SingleOutputStreamOperator<Rule> ruleStream = env.socketTextStream("192.168.20.126", 8888)
                .map(r -> {
                    String[] fields = r.split(",");
                    // rule name, first shape, second shape
                    return new Rule(fields[0], Shape.valueOf(fields[1]), Shape.valueOf(fields[2]));

                });


        // key the items by color
        KeyedStream<Item, Color> colorPartitionedStream = itemStream
                .keyBy(new KeySelector<Item, Color>() {

                    @Override
                    public Color getKey(Item value) throws Exception {
                        return value.getColor();
                    }
                });


        // a map descriptor to store the name of the rule (string) and the rule itself.
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(
                        new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {
                            @Override
                            public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {

                            }
                        }
                );


        env.execute();

    }
}
