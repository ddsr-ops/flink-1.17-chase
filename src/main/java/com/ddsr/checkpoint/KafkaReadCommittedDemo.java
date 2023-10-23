package com.ddsr.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;

/**
 *
 * Consume kafka topic using transaction level read-committed
 *
 * @author ddsr, created it at 2023/10/23 17:39
 */
public class KafkaReadCommittedDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 消费 在前面使用两阶段提交写入的Topic, refer to KafkaEOSDemo.java
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("atguigu")
                .setTopics("ws")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                // 作为 下游的消费者，要设置 事务的隔离级别 = 读已提交
                /**
                 * Controls how to read messages written transactionally. If set to read_committed, consumer.poll() will only return
                 * transactional messages which have been committed. If set to read_uncommitted (the default), consumer.poll() will return all messages, even transactional messages
                 * which have been aborted. Non-transactional messages will be returned unconditionally in either mode. Messages will always be returned in offset order. Hence, in
                 * read_committed mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction.
                 * In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, read_committed
                 * consumers will not be able to read up to the high watermark when there are in flight transactions.Further, when in read_committed the seekToEnd method will return the LSO
                 */
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();


        env
                .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkasource")
                .print();

        env.execute();
    }


}
