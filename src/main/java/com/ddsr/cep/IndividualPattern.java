package com.ddsr.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/10 21:19
 */
public class IndividualPattern {
    public static void main(String[] args) throws Exception {
        // Initialize a socket data stream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("192.168.20.126", 7777);

        // Define a CEP pattern, expecting a string of length 3 with  4 occurrences
        Pattern<String, String> pattern = Pattern.<String>begin("first")
                .where(SimpleCondition.of(s -> s.length() == 3))
                // exact 4 occurrences
//                .times(4, 4);
                // expecting 2, 3 or 4 occurrences
                // .times(2, 4);
                // exact 4 occurrences , each event arrives no more than 3 seconds after the previous event.
                .times(4, Time.seconds(3));

        // test case: aaa b, output b
        // test case: aaa ccc b, output {first=[aaa, ccc], second=[b]} and b
        pattern = Pattern.<String>begin("first")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .times(2).optional()
                .next("second")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // expecting 2, 3 or 4 occurrences and repeating as many as possible
        pattern = Pattern.<String>begin("first")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .times(2,4)
                .greedy();
        // aaa bbb ccc 23 ddd
        // Output of Non-greedy is equal to output of greedy
        //{first=[aaa, bbb]}
        //{first=[aaa, bbb, ccc]}
        //{first=[bbb, ccc]}
        //{first=[aaa, bbb, ccc, ddd]}
        //{first=[bbb, ccc, ddd]}
        //{first=[ccc, ddd]}


        /**
         *
         * The greedy() method in Flink's CEP library does not change the behavior of matching patterns in the way I described earlier.
         * The greedy() method is applied to an optional state and makes it "greedy" which means that it will take precedence over the following state.
         * Regardless of whether greedy() is applied or not, for a pattern with .times(2, 4), Flink's CEP library will try to match as many events as possible
         * within the specified range (2 to 4 in your case).If the pattern is greedy(), and it's used with an optional state, then it will try to match
         * as many events as possible for this state before moving on to the next state,but in your case, there's only one state, so greedy() doesn't change the behavior.
         * That's why you're seeing the same output for both cases. I apologize for the oversight in my previous responses.I hope this clears up the confusion.
         *
         * The "23" is between "ccc" and "ddd", why does it not break the match?
         * In Flink's Complex Event Processing (CEP) library, the pattern matching does not necessarily need to occur on consecutive events. There can be other
         * events in between the events that match a pattern.
         * In your case, the pattern is defined to match strings of length 3, and you have specified that it should match 2 to 4 times. When the input "23" comes,
         * it does not match the pattern, so it is simply ignored by the pattern matcher. The next event "ddd" does match the pattern, so it can be included in a
         * sequence with any of the previous matching events that are still within the reach of the pattern. So, the "23" does not break the match, it's just skipped
         * over because it does not meet the pattern condition. This is why you can get matches ccc, ddd and bbb, ccc, ddd after the "23" event. If you want the
         * pattern to match strictly consecutively, you can use next() instead of where() in the pattern definition.
         * Here's how you can define the pattern to enforce strict consecutive match:
         *
         * Copied! ✅
         * 📝 Copy Code
         * Pattern<String, ?> pattern = Pattern.<String>begin("start")
         *     .where(new SimpleCondition<String>() {
         *       @Override
         *       public boolean filter(String value) throws Exception {
         *         return value.length() == 3;
         *       }
         *     })
         *     .next("middle")
         *     .where(new SimpleCondition<String>() {
         *       @Override
         *       public boolean filter(String value) throws Exception {
         *         return value.length() == 3;
         *       }
         *     })
         *     .times(1, 3);
         * With this definition, any event that does not match the condition will break the sequence.
         *
         *
         */

        /*
         * test case: aaa bbb ccc 23 ddd
         * {start=[aaa], middle=[bbb]}
         * {start=[aaa], middle=[bbb, ccc]}
         * {start=[bbb], middle=[ccc]}
         * {start=[aaa], middle=[bbb, ccc, ddd]}
         * {start=[bbb], middle=[ccc, ddd]}
         */
        pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.length() == 3;
                    }
                })
                .next("middle")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.length() == 3;
                    }
                })
                .times(1, 3);

        pattern = Pattern.<String>begin("start")
                .where(SimpleCondition.of(value -> value.length() == 3))
                .times(3); // consecutive or non-consecutive
//                .consecutive();

        PatternStream<String> patternStream = CEP.pattern(ds, pattern)
                .inProcessingTime();

        SingleOutputStreamOperator<String> resultStream = patternStream.process(new PatternProcessFunction<String, String>() {

            @Override
            public void processMatch(Map<String, List<String>> match, Context ctx, Collector<String> out) {
                // print the content of match
//                System.out.println(match.toString());
//                String result = match.get("first").get(0) // Maybe NPE due to the optional of first condition
//                        + " "
//                        + match.get("first").get(1) // Maybe NPE due to the optional of first condition
//                        + " "
//                        + match.get("second").get(0);
//                        + " "
//                        + match.get("first").get(2) // corresponding to time(4)
//                        + " "
//                        + match.get("first").get(3);  // corresponding to time(4)
                out.collect(match.toString());
            }
        });


        resultStream.print();


        env.execute();
    }
}
