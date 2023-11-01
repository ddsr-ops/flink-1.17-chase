-- https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kafka/

CREATE TABLE KafkaTable (
                            `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp', -- R/W 'timestamp' is Timestamp of the Kafka record.
                            `partition` BIGINT METADATA VIRTUAL, -- Only readable permitted, must be written explicitly here
                            `offset` BIGINT METADATA VIRTUAL, -- Only readable permitted, must be written explicitly here
                            `user_id` BIGINT,
                            `item_id` BIGINT,
                            `behavior` STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv'
      );

-- What metadata columns are writable or only readable, see the reference of the first line
-- If customized column name is the same as the metadata column name , `from metadata-column` is unnecessary to be written.
-- For example, column `offset` is defined column in KafkaTable, so `from offset`(offset is a metadata column name) is not needed to be written.

CREATE TABLE MyTable (
                         `user_id` BIGINT,
                         `name` STRING,
-- 将时间戳强转为 BIGINT
                         `timestamp` BIGINT METADATA -- convert timestamp to bigint
) WITH (
      'connector' = 'kafka'
      ...
      );
-- 如果自定义列的数据类型和 Connector 中定义的 metadata 字段的数据类型不一致，程序运行时会自动 cast强转，但是这要求两种数据类型是可以强转的。


CREATE TABLE KafkaTable (
                            `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
                            `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
                            `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  -- from Kafka connector
                            `offset` BIGINT METADATA VIRTUAL,  -- from Kafka connector
                            `user_id` BIGINT,
                            `item_id` BIGINT,
                            `behavior` STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'debezium-json'
      );

-- This example shows how to access both Kafka and Debezium metadata fields
-- from Debezium format indicates the column is from debezium format, field timestamp or table are written by Debezium, for more details refer to Debezium doc
-- from Kafka connector indicates the column is from Kafka format
