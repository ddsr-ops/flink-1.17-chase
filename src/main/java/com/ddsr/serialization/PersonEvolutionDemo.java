package com.ddsr.serialization;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Flink supports evolving schema of POJO types, based on the following set of rules:
 * <p>
 * <li>1. Fields can be removed. Once removed, the previous value for the removed field will be dropped in future
 * checkpoints and savepoints.</li>
 * <li>2. New fields can be added. The new field will be initialized to the default value for its type, as defined by
 * Java.</li>
 * <li>3. Declared fields types cannot change.</li>
 * <li>4. Class name of the POJO type cannot change, including the namespace of the class.</li>
 * Note that the schema of POJO type state can only be evolved when restoring from a previous savepoint with Flink
 * versions newer than 1.8.0. When restoring with Flink versions older than 1.8.0, the schema cannot be changed.
 *
 * @author ddsr, created it at 2024/12/24 16:46
 */
public class PersonEvolutionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("person-topic", new SimpleStringSchema(),
                properties);
        DataStream<String> stream = env.addSource(consumer);

        DataStream<Person> personStream = stream.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) {
                String[] fields = value.split(",");
                return new Person(fields[0], fields[1]); // first version
//                return new Person(fields[0], fields[1], fields[2]); // second version after person schema evolution
                // For detail, you can reference PersonEvolution
                // Note, the PersonEvolution is an evolution of Person, but never be used here.
                // Because Class name of the POJO type cannot change, including the namespace of the class
            }
        });

        personStream.print();

        env.execute("Schema Evolution Flink Job");
    }
}
