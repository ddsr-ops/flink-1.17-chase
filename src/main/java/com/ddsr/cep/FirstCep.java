package com.ddsr.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author ddsr, created it at 2023/12/7 22:43
 */
public class FirstCep {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> socketDataStream = env.socketTextStream("192.168.20.126", 7777);

//        socketDataStream.print();

        // test case: aaa b
        Pattern<String, String> cepPattern = Pattern.<String>begin("first")
                .where( SimpleCondition.of(s -> s.length() == 3))
                .next("second")
                .where(SimpleCondition.of(s -> s.startsWith("b")));

        // test case: aaa aa x
//        Pattern<String, ?> cepPattern = Pattern.<String>begin("start")
//                .where(SimpleCondition.of(s -> s.length() == 3))
//                .next("middle")
//                .subtype(String.class)
//                .where(SimpleCondition.of(s -> s.length() == 2))
//                .followedBy("end")
//                .where(SimpleCondition.of(s -> s.startsWith("x")));

        /*

          Reference: https://stackoverflow.com/questions/68089043/apache-flink-pattern-detection-does-not-find-any-match/68089972#68089972

          FlinkCEP by default uses EventTime which relies on a WaterMark to help those events proceeding. It may helps if you switch it to ProcessingTime.

          Important Note:<p>

          CEP relies on being able to sort the event stream by timestamp. This requires that you either (1) use events that have timestamps and provide a WatermarkStrategy,
          or (2) specify that you want the pattern matching to be done in ingestion order, using processing time semantics.

          You can do the latter by making this small change:
         */
        PatternStream<String> patternStream = CEP.pattern(socketDataStream, cepPattern).inProcessingTime();


        // use process function to fetch result alternatively instead of select method

        DataStream<String> resultStream = patternStream.process(
                new PatternProcessFunction<String, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<String>> pattern,
                            Context ctx,
                            Collector<String> out) {
                        String result = pattern.get("first").get(0)
                                + " "
                                + pattern.get("second").get(0);
                        out.collect(result);
                    }
                });
//        DataStream<String> resultStream = patternStream.select(new PatternSelectFunction<String, String>() {
//            @Override
//            public String select(Map<String, List<String>> pattern) {
//                return pattern.getOrDefault("first", new ArrayList<>()).get(0) + " " + pattern.getOrDefault("second", new ArrayList<>()).get(0);
//            }
//        });

        resultStream.print();

        env.execute();


    }

}
