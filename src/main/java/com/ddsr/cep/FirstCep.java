package com.ddsr.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author ddsr, created it at 2023/12/7 22:43
 */
public class FirstCep {
    public static void main(String[] args) {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // todo: initialize a socket data stream from host 88.88.16.126 with port 7777

        // todo: Define a CEP pattern, the length of the first string is 3, the next string starts with 'b'

        // todo: apply the CEP pattern to the socket data stream

        // todo: fetch result applied CEP pattern , then print it

        // print "I am out"
        System.out.println("I am out");



    }

    public static void CEP(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketDataStream = env.socketTextStream("88.88.16.126", 7777);

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

        DataStream<String> resultStream = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) {
                return pattern.get("first").get(0) + " " + pattern.get("second").get(0);
            }
        });

        resultStream.print();

        env.execute();
    }
}
