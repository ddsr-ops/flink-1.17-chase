package com.ddsr.cep.amss;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.NoSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * For a given pattern, the same event may be assigned to multiple successful matches.
 * To control how many matches an event will be assigned, you need to specify the skip
 * strategy called {@code AfterMatchSkipStrategy}. There are five types of skip strategies,
 * listed as follows:
 * <ul>
 *   <li>{@code NO_SKIP}: Every possible match will be emitted.</li>
 *   <li>{@code SKIP_TO_NEXT}: Discards every partial match that started with the same event
 *       as the emitted match.</li>
 *   <li>{@code SKIP_PAST_LAST_EVENT}: Discards every partial match that started after the
 *       match started but before it ended.</li>
 *   <li>{@code SKIP_TO_FIRST}: Discards every partial match that started after the match
 *       started but before the first event of {@code PatternName} occurred.</li>
 *   <li>{@code SKIP_TO_LAST}: Discards every partial match that started after the match
 *       started but before the last event of {@code PatternName} occurred.</li>
 * </ul>
 * <p>The first three strategies are for <strong>matches</strong>, the last two strategies are
 * for <strong>events</strong>.</p>
 * Note that when using {@code SKIP_TO_FIRST} and {@code SKIP_TO_LAST} skip strategy, a valid
 * {@code PatternName} should also be specified.
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/#after-match-skip-strategy">after-match-skip-strategy</a>
 */
public class AfterMatchSkipStrategyDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        NoSkipStrategy noSkipStrategy = AfterMatchSkipStrategy.noSkip();

        // Test case: a1 a2 a3 b
        // Output:
        // {start=[a1, a2, a3], middle=[b]}
        //{start=[a1, a2], middle=[b]}
        //{start=[a1], middle=[b]}
        //{start=[a2, a3], middle=[b]}
        //{start=[a2], middle=[b]}
        //{start=[a3], middle=[b]}
        Pattern<String, String> pattern = Pattern.<String>begin("start", noSkipStrategy)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        // Greedy
        // Test case: a1 a2 a3 b
        // Output:
        // {start=[a1, a2, a3], middle=[b]}
        // {start=[a2, a3], middle=[b]}
        // {start=[a3], middle=[b]}
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .greedy()
                .followedBy("middle")
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
