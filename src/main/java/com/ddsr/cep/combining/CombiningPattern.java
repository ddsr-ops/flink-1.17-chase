package com.ddsr.cep.combining;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * <h1>CombiningPattern Class</h1>
 *
 * <p>The CombiningPattern class demonstrates the use of different forms of contiguity between events supported by FlinkCEP:</p>
 *
 * <ul>
 *   <li><b>Strict Contiguity</b>: Expects all matching events to appear strictly one after the other, without any non-matching events in-between.</li>
 *   <li><b>Relaxed Contiguity</b>: Ignores non-matching events appearing in-between the matching ones.</li>
 *   <li><b>Non-Deterministic Relaxed Contiguity</b>: Further relaxes contiguity, allowing additional matches that ignore some matching events.</li>
 * </ul>
 *
 * <p>To apply these forms of contiguity between consecutive patterns, use the following methods:</p>
 *
 * <ul>
 *   <li><code>next()</code>: for strict contiguity,</li>
 *   <li><code>followedBy()</code>: for relaxed contiguity, and</li>
 *   <li><code>followedByAny()</code>: for non-deterministic relaxed contiguity.</li>
 * </ul>
 *
 * <p>Alternatively, use the following methods if you want to prevent certain events from following others:</p>
 *
 * <ul>
 *   <li><code>notNext()</code>: to prevent an event type from directly following another, and</li>
 *   <li><code>notFollowedBy()</code>: to prevent an event type from being anywhere between two other event types.</li>
 * </ul>
 *
 * <p><b>Note:</b> A pattern sequence cannot end with <code>notFollowedBy()</code> unless the time interval is defined via <code>withIn()</code>. Also, a NOT pattern cannot be preceded by an optional one.</p>
 *
 * <p><b>Example:</b></p>
 * <blockquote><pre>
 * // strict contiguity
 *  Pattern<Event, ?> strict = start.next("middle").where(...);
 * // relaxed contiguity
 * Pattern<Event, ?> relaxed = start.followedBy("middle").where(...);
 * // non-deterministic relaxed contiguity
 * Pattern<Event, ?> nonDetermin = start.followedByAny("middle").where(...);
 * // NOT pattern with strict contiguity
 * Pattern<Event, ?> strictNot = start.notNext("not").where(...);
 * // NOT pattern with relaxed contiguity
 * Pattern<Event, ?> relaxedNot = start.notFollowedBy("not").where(...);
 *</pre></blockquote>
 *
 *
 * <p>
 * <strong>Relaxed contiguity</strong> means that only the first succeeding
 * matching event will be matched. However, with <strong>non-deterministic
 * relaxed contiguity</strong>, multiple matches will be emitted for the same
 * beginning.
 * </p>
 *
 * <p>
 * For example, consider a pattern "a b" and the event sequence "a", "c",
 * "b1", "b2". This will result in the following:
 * </p>
 *
 * <ul>
 *   <li>
 *   <strong>Strict Contiguity</strong> between "a" and "b": {} (no match).
 *   The "c" after "a" causes "a" to be discarded.
 *   </li>
 *   <li>
 *   <strong>Relaxed Contiguity</strong> between "a" and "b": {a b1}.
 *   Relaxed contiguity is viewed as "skip non-matching events till the next
 *   matching one".
 *   </li>
 *   <li>
 *   <strong>Non-Deterministic Relaxed Contiguity</strong> between "a" and
 *   "b": {a b1}, {a b2}. This is the most general form.
 *   </li>
 * </ul>
 *
 * <p>
 * It's also possible to define a temporal constraint for the pattern to be
 * valid. For example, a pattern should occur within 10 seconds could be
 * defined via the <code>pattern.within()</code> method. Temporal patterns
 * are supported for both processing and event time.
 * </p>
 *
 * <p>
 * Please note that a pattern sequence can only have one temporal constraint.
 * If multiple such constraints are defined on different individual patterns,
 * then the smallest one is applied.
 * </p>
 *
 *
 * <hr>
 * <p><i>Author: ddsr, created on 2023/12/15 21:12</i></p>
 */
public class CombiningPattern {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // Define a pattern , matching the before event starting with 'a', the next event starting with 'b'.
        // The two events are strictly consecutive.
        // Test case: a c b1 b2, there is no any output in this case.
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .next("middle")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // Define a pattern , matching the before event starting with 'a', the next event starting with 'b'.
        // The two events are relaxed consecutive.
        // Test case: a c b1 b2, output: {start=[a], middle=[b1]}
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .followedBy("middle")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // Define a pattern using followedByAny, matching the before event starting with 'a', the next event starting
        // with 'b', non-deterministic contiguity
        // Test case: a c b1 b2 d b3 a b4, output:
        // {start=[a], middle=[b1]}
        //{start=[a], middle=[b2]}
        //{start=[a], middle=[b3]}
        //{start=[a], middle=[b4]}
        //{start=[a], middle=[b4]}
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .followedByAny("middle")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // Define a pattern, matching the before event starting with 'a', the following event without starting with 'b'
        // within 3 seconds.
        // A pattern sequence cannot end with notFollowedBy() if the time interval is not defined via withIn().
        // Test case: a c1 c2 ... , output : a
        // Even if input the only one a, the 'a' will be output in 3 seconds.
        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .notFollowedBy("middle")
                .where(SimpleCondition.of(s -> s.startsWith("b")))
                .within(Time.seconds(3));

        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(s -> s.startsWith("a")))
                .next("middle")
                .where(SimpleCondition.of(s -> s.startsWith("b")))
                .oneOrMore()
                .next("end")
                .where(SimpleCondition.of(s -> s.startsWith("c")));



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
