CREATE TABLE t1
(
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    --列名和元数据名一致可以省略 FROM 'xxxx', VIRTUAL表示只读
    `partition`  BIGINT METADATA VIRTUAL,
    `offset`     BIGINT METADATA VIRTUAL,
    id           int,
    ts           bigint,
    vc           int
)
    WITH (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = 'hadoop103:9092',
        'properties.group.id' = 'atguigu',
-- 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'
        'scan.startup.mode' = 'earliest-offset',
        -- fixed为flink实现的分区器，一个并行度只写往kafka一个分区
        'sink.partitioner' = 'fixed',
        'topic' = 'ws1',
        'format' = 'json'
        )

-- sink partitioner
-- Output partitioning from Flink's partitions into Kafka's partitions. Valid values are
-- default: use the kafka default partitioner to partition records.
-- fixed: each Flink partition ends up in at most one Kafka partition.
-- round-robin: a Flink partition is distributed to Kafka partitions sticky round-robin. It only works when record's keys are not specified.
-- Custom FlinkKafkaPartitioner subclass: e.g. 'org.mycompany.MyPartitioner'.


-- 如果当前表存在更新操作，那么普通的kafka连接器将无法满足，此时可以使用Upsert Kafka连接器。
-- Upsert Kafka 连接器支持以 upsert 方式从 Kafka topic 中读取数据并将数据写入 Kafka topic。
-- 作为 source，upsert-kafka 连接器生产 changelog 流，其中每条数据记录代表一个更新或删除事件。更准确地说，数据记录中的 value 被解释为同一 key 的最后一个 value 的 UPDATE，如果有这个 key（如果不存在相应的 key，则该更新被视为 INSERT）。用表来类比，changelog 流中的数据记录被解释为 UPSERT，也称为 INSERT/UPDATE，因为任何具有相同 key 的现有行都被覆盖。另外，value 为空的消息将会被视作为 DELETE 消息。
-- 作为 sink，upsert-kafka 连接器可以消费 changelog 流。它会将 INSERT/UPDATE_AFTER 数据作为正常的 Kafka 消息写入，并将 DELETE 数据以 value 为空的 Kafka 消息写入（表示对应 key 的消息被删除）。Flink 将根据主键列的值对数据进行分区，从而保证主键上的消息有序，因此同一主键上的更新/删除消息将落在同一分区中。

-- （1）创建upsert-kafka的映射表(必须定义主键)
CREATE TABLE t2(
    id int ,
    sumVC int ,
    primary key (id) NOT ENFORCED
)
WITH (
  'connector' = 'upsert-kafka',
  'properties.bootstrap.servers' = 'hadoop102:9092',
  'topic' = 'ws2',
  'key.format' = 'json',
  'value.format' = 'json'
)