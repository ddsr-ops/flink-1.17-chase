package com.ddsr.cep.individual;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/13 22:24
 */
public class IterativeConditionPattern {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // Iterative conditions
        // case: take int-string as input, match the events where the int is continuously increasing
        // input: 1 2 2 3 4 5 6 7 8 9
        Pattern<String, ?> pattern = Pattern.<String>begin("start")
                .next("middle")
                .where(new IterativeCondition<String>() {
                    // The call to ctx.getEventsForPattern(...) finds all the previously accepted events for a given
                    // potential match. The cost of this operation can vary, so when implementing your condition, try to minimize its use.
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        return Integer.parseInt(value) >= Integer.parseInt(ctx.getEventsForPattern("start").iterator().next());
                    }
                })
                .next("end")
                .where(new IterativeCondition<String>() {
                    // The call to ctx.getEventsForPattern(...) finds all the previously accepted events for a given
                    // potential match. The cost of this operation can vary, so when implementing your condition, try to minimize its use.
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        return Integer.parseInt(value) >= Integer.parseInt(ctx.getEventsForPattern("middle").iterator().next());
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        patternStream.process(new PatternProcessFunction<String, String>() {

            @Override
            public  void processMatch(Map<String, List<String>> match, Context ctx, Collector<String> out) {
                out.collect(match.toString());
            }
        }).print();

        env.execute();


    }
}
