package com.ddsr.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/15 22:39
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers("ora11g:9092")
                .setGroupId("ddsr")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("topic1")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .print();

        env.execute();

    }
}


/**
 * Kafka consumers - auto.offset.reset
 * earliest :  consume from offset if exists, otherwise from the earliest offset
 * latest   :  consume from offset if exists, otherwise the the latest offset
 * <p>
 * OffsetsInitializer.latest(): always consume from latest offset, distinguish from auto.offset.reset of kafka consumers.
 * OffsetsInitializer.earliest(): always consume from latest offset, distinguish from auto.offset.reset of kafka consumers.
 */