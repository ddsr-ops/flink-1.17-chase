package com.ddsr.cep.group;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 *
 * @author ddsr created it at 2023-12-20 09:40:27
 */
public class GroupsOfPatterns {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // GroupPattern of begin
        // Test case: a c bx by, output: a bx
        GroupPattern<String, String> pattern = Pattern.begin(
                Pattern.<String>begin("start")
                        .where(SimpleCondition.of(s -> s.startsWith("a")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(s -> s.startsWith("b")))
        );

        PatternStream<String> patternStream = CEP.pattern(ds, pattern).inProcessingTime();

        patternStream.process(new PatternProcessFunction<String, String>() {
            @Override
            public void processMatch(Map<String, List<String>> match, Context ctx, Collector<String> out) {
                out.collect(match.toString());
            }
        }).print();
        env.execute();

    }
}
