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
        'sink.partitioner' = 'fixed', -- todo: 为什么这里是fixed?
        'topic' = 'ws1',
        'format' = 'json'
        )