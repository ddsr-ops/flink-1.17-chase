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
                // exact 4 occurrences , each event arrives no more than 3 seconds after the previous event.
                .times(4, Time.seconds(3));

        pattern = Pattern.<String>begin("first")
                .where(SimpleCondition.of(s -> s.length() == 3))
                .times(2).optional()
                .next("second")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

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
