CREATE TABLE t3
(
    id int,
    ts bigint,
    vc int
)
    WITH (
        'connector' = 'filesystem',
        'path' = 'hdfs://hadoop102:8020/data/t3',-- hdfs
        'format' = 'csv'
        );

create table t4
(
    id   int,
    name string
)
    with (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/t4',
        'format' = 'csv'
        );