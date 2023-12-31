package com.ddsr.cep.individual;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/12 22:35
 */
public class IndividualPattern1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .oneOrMore(); // expect one or more occurrences, a relaxed contiguity

        // test case: 1 b 22 b 333 b3, output:
        // {next=[b]}
        // {next=[b]}
        // {start=[333], next=[b3]}
        // {next=[b3]}
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .oneOrMore() // a relaxed continuity
                .optional()
                .next("next")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // test case: 1 b 222 b 333 b3 b4 555 b5, output:
        // The next continuity of timesOrMore is a string starting with b
        // output: 222 333 b3; 222 333 555 b5; 333 555 b5
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .timesOrMore(2) // a relaxed continuity
                .optional() // zero , 2 or more occurrences
                .next("next")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

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
