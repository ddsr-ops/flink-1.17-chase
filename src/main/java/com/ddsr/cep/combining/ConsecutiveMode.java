package com.ddsr.cep.combining;

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
 * <p>For looping patterns (e.g. oneOrMore() and times()) the default is relaxed contiguity. If you want strict contiguity,
 * you have to explicitly specify it by using the <code>consecutive()</code> call, and if you want non-deterministic
 * relaxed contiguity you can use the <code>allowCombinations()</code> call.</p>
 *
 * <p><b>consecutive()</b></p>
 * <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes strict contiguity between
 * the matching events, i.e. any non-matching element breaks the match (as in next()). If not applied a relaxed contiguity
 * (as in followedBy()) is used.</p>
 *
 * <p><b>allowCombinations()</b></p>
 * <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes non-deterministic relaxed
 * contiguity between the matching events (as in followedByAny()). If not applied a relaxed contiguity
 * (as in followedBy()) is used.</p>
 *
 * @author ddsr, created it at 2023/12/19 18:20
 */
public class ConsecutiveMode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // Test case: c d a1 a2 a3 d a4 b
        // Output:
        // {start=[c], middle=[a1, a2, a3], end=[b]}
        // {start=[c], middle=[a1], end=[b]}
        // {start=[c], middle=[a1, a2], end=[b]}
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(value -> value.startsWith("c")))
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .consecutive()
                .followedBy("end")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        // Test case: c d a1 a2 a3 d a4 b
        // Output:
        // {start=[c], middle=[a1, a2, a3, a4], end=[b]}
        // {start=[c], middle=[a1], end=[b]}
        // {start=[c], middle=[a1, a4], end=[b]}
        // {start=[c], middle=[a1, a3], end=[b]}
        // {start=[c], middle=[a1, a3, a4], end=[b]}
        // {start=[c], middle=[a1, a2], end=[b]}
        // {start=[c], middle=[a1, a2, a4], end=[b]}
        // {start=[c], middle=[a1, a2, a3], end=[b]}
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(value -> value.startsWith("c")))
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .allowCombinations()
                .followedBy("end")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

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
