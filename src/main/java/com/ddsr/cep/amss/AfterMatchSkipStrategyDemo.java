package com.ddsr.cep.amss;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.NoSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipToFirstStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipToLastStrategy;
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
 * @see <a href="https://blog.csdn.net/weixin_45417821/article/details/124754226">introduction for after-match-skip-strategy</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/#after-match-skip-strategy">
 *     after-match-skip-strategy</a>
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
        // {start=[a1, a2], middle=[b]}
        // {start=[a1], middle=[b]}
        // {start=[a2, a3], middle=[b]}
        // {start=[a2], middle=[b]}
        // {start=[a3], middle=[b]}
        // noSkipStrategy is the default strategy
        // 不跳过（NO_SKIP）
        // 代码调用 AfterMatchSkipStrategy.noSkip()。这是默认策略，所有可能的匹配都会输出。所以这里会输出完整的 6 个匹配。
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
        // 应该检测到 6 个匹配结果：（a1 a2 a3 b），（a1 a2 b），（a1 b），（a2 a3 b），（a2 b），（a3 b）。如果在初始模式的量词.
        // oneOrMore()后加上.greedy()定义为贪心匹配，那么结果就是：（a1 a2 a3 b），（a2 a3 b），（a3 b），每个事件作为开头只会出现一次。
        pattern = Pattern.<String>begin("start", noSkipStrategy)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .greedy()
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        AfterMatchSkipStrategy skipToNext = AfterMatchSkipStrategy.skipToNext();

        // Test case: a1 a2 a3 b
        // Output:
        // {start=[a1, a2, a3], middle=[b]}
        // {start=[a2, a3], middle=[b]}
        // {start=[a3], middle=[b]}
        // 跳至下一个（SKIP_TO_NEXT）
        // 找到一个 a1 开始的最大匹配之后，跳过a1 开始的所有其他匹配，直接从下一个 a2 开始匹配起。当然 a2 也是如此跳过其他匹配。
        // 最终得到（a1 a2 a3 b），（a2 a3 b），（a3 b）。可以看到，这种跳过策略跟使用.greedy()效果是相同的。
        pattern = Pattern.<String>begin("start", skipToNext)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        AfterMatchSkipStrategy skipPastLastEvent = AfterMatchSkipStrategy.skipPastLastEvent();

        // Test case: a1 a2 a3 b
        // Output: {start=[a1, a2, a3], middle=[b]}
        // 跳过所有子匹配（SKIP_PAST_LAST_EVENT）
        //代码调用 AfterMatchSkipStrategy.skipPastLastEvent()。找到 a1 开始的匹配（a1 a2 a3 b）之后，直接跳过所有 a1 直到 a3
        // 开头的匹配，相当于把这些子匹配都跳过了。最终得到（a1 a2 a3 b），这是最为精简的跳过策略。
        pattern = Pattern.<String>begin("start", skipPastLastEvent)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        SkipToFirstStrategy skipToFirstStrategy = AfterMatchSkipStrategy.skipToFirst("start");

        // Test case: a1 a2 a3 b
        // Output: {start=[a1, a2, a3], middle=[b]}
        // {start=[a1, a2], middle=[b]}
        // {start=[a1], middle=[b]}
        // 跳至第一个（SKIP_TO_FIRST[a]）
        // 代码调用 AfterMatchSkipStrategy.skipToFirst(“start”)，这里传入一个参数，指明跳至哪个模式的第一个匹配事件。找到a1开始的匹配
        // （a1 a2 a3 b）后，跳到以最开始一个 a（也就是 a1）为开始的匹配，相当于只留下 a1 开始的匹配。最终得到（a1 a2 a3 b），
        // （a1 a2 b），（a1 b）。
        pattern = Pattern.<String>begin("start", skipToFirstStrategy)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
                .followedBy("middle")
                .where(SimpleCondition.of(value -> value.startsWith("b")));

        SkipToLastStrategy skipToLastStrategy = AfterMatchSkipStrategy.skipToLast("start");

        // Test case: a1 a2 a3 b
        // Output: {start=[a1, a2, a3], middle=[b]}
        // {start=[a3], middle=[b]}
        // 跳至最后一个（SKIP_TO_LAST[a]）
        // 代码调用 AfterMatchSkipStrategy.skipToLast(“start”)，同样传入一个参数，指明跳至哪个模式的最后一个匹配事件。找到a1开始的匹配
        // （a1 a2 a3 b）后，跳过所有 a1、a2 开始的匹配，跳到以最后一个 a（也就是 a3）为开始的匹配。最终得到（a1 a2 a3 b），（a3 b）。
        // 1. The pattern will initially match "a1", "a2", and "a3" as part of the start condition, followed by "b" for
        // the middle condition, resulting in the first match: ["a1", "a2", "a3", "b"].
        // 2. With the AfterMatchSkipStrategy.skipToLast("start"), the next match attempt will start after the last
        // element that satisfied the start condition in the previous match, which is "a3".
        // 3. The second match attempt will then immediately match "a3" as the start condition and "b" as the middle
        // condition, giving the second match: ["a3", "b"]
        pattern = Pattern.<String>begin("start", skipToLastStrategy)
                .where(SimpleCondition.of(value -> value.startsWith("a")))
                .oneOrMore()
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
