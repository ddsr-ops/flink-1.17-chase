package com.ddsr.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
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

        Pattern<String, ?> cepPattern = Pattern.<String>begin("first")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.length() == 3;
                    }
                })
                .next("second")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("b");
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(socketDataStream, cepPattern);

        // use process function to fetch result alternatively instead of select method

//        DataStream<String> resultStream = patternStream.process(
//                new PatternProcessFunction<String, String>() {
//                    @Override
//                    public void processMatch(
//                            Map<String, List<String>> pattern,
//                            Context ctx,
//                            Collector<String> out) throws Exception {
//                        String result = pattern.get("first").get(0)
//                                + " "
//                                + pattern.get("second").get(0);
//                        out.collect(result);
//                    }
//                });
        DataStream<String> resultStream = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) {
                return pattern.getOrDefault("first", new ArrayList<>()).get(0) + " " + pattern.getOrDefault("second", new ArrayList<>()).get(0);
            }
        });

        resultStream.print();

        env.execute();


    }

}
