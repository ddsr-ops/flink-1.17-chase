package com.ddsr.cep.group;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
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

        // GroupPattern of begin, comment the following patterns after this pattern if testing, and apply the start pattern
        // to the ds stream
        // Test case: a c bx by, output: a bx
        GroupPattern<String, String> start = Pattern.begin(
                Pattern.<String>begin("start1")
                        .where(SimpleCondition.of(s -> s.startsWith("a")))
                        .followedBy("middle1")
                        .where(SimpleCondition.of(s -> s.startsWith("b")))
        );

        // GroupPattern of next
        // Test case: a c b1 3 c1 4 d2 a c b1 c 2 d1, output: {start1=[a], middle1=[b1], start2=[c], middle2=[d1]}
        // Note: the '3' breaks the next match, so no output for a b1 c1 d2
        Pattern<String, String> pattern = start
                .next(
                        Pattern.<String>begin("start2")
                                .where(SimpleCondition.of(s -> s.startsWith("c")))
                                .followedBy("middle2")
                                .where(SimpleCondition.of(s -> s.startsWith("d")))
                );

        // GroupPattern of followedBy
        // Test case: a c b1 3 c1 4 d2 a c b1 c 2 d1,
        // Output: {start1=[a], middle1=[b1], start3=[c1], middle3=[d2]}
        // {start1=[a], middle1=[b1], start3=[c], middle3=[d1]}
        pattern = start
                .followedBy(
                        Pattern.<String>begin("start3")
                                .where(SimpleCondition.of(s -> s.startsWith("c")))
                                .followedBy("middle3")
                                .where(SimpleCondition.of(s -> s.startsWith("d")))
                );

        // GroupPattern of followedByAny
        // Test case: a1 c1 b1 3 c2 4 d1 a2 c3 b3 c2 2 d2,
        // Output: {start1=[a1], middle1=[b1], start4=[c2], middle4=[d1]}
        //{start1=[a1], middle1=[b1], start4=[c3], middle4=[d2]}
        //{start1=[a1], middle1=[b1], start4=[c2], middle4=[d2]}
        //{start1=[a2], middle1=[b3], start4=[c2], middle4=[d2]}
        pattern = start
                .followedByAny(
                        Pattern.<String>begin("start4")
                                .where(SimpleCondition.of(s -> s.startsWith("c")))
                                .followedBy("middle4")
                                .where(SimpleCondition.of(s -> s.startsWith("d")))
                );

        // Not Next
        // Test case: a x b c d a y b s
        // Output : {start1=[a], middle1=[b]}, due to a y b s
        pattern = start
                .notNext("not_next")
                .where(SimpleCondition.of(s -> s.startsWith("c")));


        // Not Followed used in conjunction with within, NotFollowedBy is not supported without windowTime as a last part
        // of a Pattern!
        // Test case: a1 x b1 c1 d a1 y b1 s a2 b2 z c3, input quickly
        // Output: {start1=[a1], middle1=[b1]}, a2 b2 will not match(output) because c3 arrives
        pattern = start
                .notFollowedBy("not_followed_by")
                .where(SimpleCondition.of(s -> s.startsWith("c")))
                .within(Time.seconds(5));

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
