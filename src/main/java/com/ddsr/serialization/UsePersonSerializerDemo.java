package com.ddsr.serialization;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Use {@link PersonSerializer} to illuminate the order of serialization and deserialization
 *
 * @author ddsr, created it at 2024/12/20 17:10
 */
public class UsePersonSerializerDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        env.getConfig().addDefaultKryoSerializer(
                Person.class,
                PersonSerializer.class
        );

        env.fromElements(
                        new Person("Fred", "male"),
                        new Person("Wilma", "female"),
                        new Person("male", "Pebbles"), // Note, sex and name are reversed
                        new Person("female", "BamBam") // Note, sex and name are reversed
                )
                .print();
        /*
        Person{name='Fred', sex='male'}
        Person{name='Wilma', sex='female'}
        Person{name='male', sex='Pebbles'}
        Person{name='female', sex='BamBam'}
         */

        env.execute();
    }
}
