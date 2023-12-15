package com.ddsr.cep.individual;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/14 21:18
 */
public class CombiningConditionPattern {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // test case: aa bbb 11 3; output: aa bbb 11
        // match the event with length 2 or 3
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.length() == 2))
                .or(SimpleCondition.of(s -> s.length() == 3));

        // Define a patter, matching the event that starts with 'a' and length is 2 or 3
//        pattern = Pattern.<String>begin("start")
//                .where(SimpleCondition.of(s -> s.startsWith("a")))
//                .where(SimpleCondition.of(s -> s.length() == 2)) // and
//                .or(SimpleCondition.of(s -> s.length() == 3)); // or

        // Define a pattern which match the event that starts with 'a' one or more occurrences, until the event starts with 'b' occurs
        // test case: "a1" "c" "a2" "b" "a3", output:
        // {start=[a1]}
        // {start=[a1, a2]}
        // {start=[a2]}
        // {start=[a3]}

        // The pattern you have mentioned is "(a+ until b)", which means it will keep accepting 'a' events until it
        // encounters a 'b'. Once 'b' is encountered, it stops processing 'a' events for that pattern and outputs the results.
        //
        //In the sequence "a1" "c" "a2" "b" "a3", when 'b' is encountered, the library outputs all possible combinations
        // of 'a' until 'b', which are {a1, a2}, {a1}, and {a2}. After 'b', 'a3' comes in and starts a new sequence,
        // hence {a3} is also output. However, because 'a3' comes after 'b', it is not considered part of the sequence
        // before 'b', so {a2, a3} is not output.
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .timesOrMore(1) // a relaxed continuity
                .until(SimpleCondition.of(s -> s.startsWith("b"))); // It allows for cleaning state for corresponding
                                                                    // pattern on event-based condition.


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
