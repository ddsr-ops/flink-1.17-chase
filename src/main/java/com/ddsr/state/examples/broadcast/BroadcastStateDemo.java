package com.ddsr.state.examples.broadcast;

import org.apache.flink.api.common.state.MapState;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * we have a stream of objects of different colors and shapes, we want to find pairs of objects of the same color
 * that follow a certain pattern, e.g. a rectangle followed by a triangle. We assume that the set of interesting
 * patterns evolves over time.
 *
 * @author ddsr, created it at 2024/12/8 16:01
 * @see <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/broadcast_state/"
 * >broadcast_state</a>
 */
@SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        // itemSteam consists of items of different colors and shapes
        SingleOutputStreamOperator<Item> itemStream = env.socketTextStream("192.168.20.126", 7777)
                .map(r -> {
                    String[] fields = r.split(",");
                    // color, shape
                    return new Item(Color.valueOf(fields[0]), Shape.valueOf(fields[1]));
                });

        // ruleStream contains pairs of shapes that are interesting
        SingleOutputStreamOperator<Rule> ruleStream = env.socketTextStream("192.168.20.126", 8888)
                .map(r -> {
                    String[] fields = r.split(",");
                    // rule name, first shape, second shape
                    return new Rule(fields[0], Shape.valueOf(fields[1]), Shape.valueOf(fields[2]));

                });


        // key the items by color, items with the same color go to the same partition
        KeyedStream<Item, Color> colorPartitionedStream = itemStream
                .keyBy(new KeySelector<Item, Color>() {

                    @Override
                    public Color getKey(Item value) {
                        return value.getColor();
                    }
                });


        // a map descriptor to store the name of the rule (string) and the rule itself.
        // declare a state descriptor to broadcast
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        // broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(ruleStateDescriptor);

        KeyedBroadcastProcessFunction<Color, Item, Rule, String> keyedBroadcastProcessFunction =
                new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

            // store rules to match, the first type parameter is rule name, the second one is rule
            private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
                    new MapStateDescriptor<>(
                            "RulesBroadcastState",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            TypeInformation.of(new TypeHint<Rule>() {
                            }));

            // store partial matches, i.e. first elements of the pair waiting for the corresponding
            // second element
            // keep a list as we have many first elements waiting, the first type parameter is rule
            // name used for joining the above rule state, the second one is list of first elements
            private final MapStateDescriptor<String, List<Item>> firstElementsStateDescriptor =
                    new MapStateDescriptor<>(
                            "FirstElementsBroadcastState",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            TypeInformation.of(new TypeHint<List<Item>>() {
                            }));

                    @Override
                    public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // readonlycontext can only read the broadcast state, not modify it
                        final Shape currentShape = value.getShape();

                        // get state from firstElementsStateDescriptor, the key of map is rule name
                        final MapState<String, List<Item>> firstElementsKeyedState =
                                getRuntimeContext().getMapState(firstElementsStateDescriptor);

                        // loop the rule state from broadcast state
                        for (Map.Entry<String, Rule> entry :
                                ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                            // get the rule
                            final Rule rule = entry.getValue();
                            // get the first shape
                            final Shape firstShape = rule.first;
                            // get the second shape
                            final Shape secondShape = rule.second;
                            // rule name
                            final String ruleName = entry.getKey();

                            List<Item> storedFirstItems = firstElementsKeyedState.get(ruleName);
                            if (storedFirstItems == null) {
                                // if there is no first elements stored, create an empty list
                                storedFirstItems = new ArrayList<>();
                            }

                            // matched
                            if(currentShape == secondShape &&  !storedFirstItems.isEmpty()) {
                                // if the current shape is the same as the second shape, and there is
                                // already a first element stored, then we have a match
                                for (Item firstItem : storedFirstItems) {
                                    out.collect(ruleName + " matches " + firstItem.getColor() + " " + firstItem.getShape() + " with " + value.getColor() + " " + value.getShape());
                                }
                            }


                            if(currentShape == firstShape) {
                                // if the current shape is the same as the first shape, then we store
                                // the current item
                                storedFirstItems.add(value);
                                // todo: the storedFirstItems might have multiple first elements, occupying much
                                //  state size
                            }

                            if (storedFirstItems.isEmpty()) {
                                firstElementsKeyedState.remove(ruleName);
                            } else {
                                firstElementsKeyedState.put(ruleName, storedFirstItems);
                            }


                        }
                    }

            @Override
            public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {
                // store the rule
                ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);

            }
        };

        colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(

                        keyedBroadcastProcessFunction
                );


        env.execute();

    }
}
