package com.ddsr.datatypes;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Person1TypeFactoryDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromElements("Alice,30", "Bob,25", "Charlie,35");

        DataStream<Person> persons = text.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) throws Exception {
                String[] parts = value.split(",");
                return new Person(parts[0], Integer.parseInt(parts[1]));
            }
        });

        persons.print();

        env.execute("Person Type Job");
    }
}