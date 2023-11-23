CREATE TABLE t4
(
    id                      INT,
    ts                   BIGINT,
    vc                     INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector'='jdbc',
      'url' = 'jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=UTF-8',
      'username' = 'root',
      'password' = '000000',
      'connection.max-retry-timeout' = '60s',
      'table-name' = 'ws2',
      'sink.buffer-flush.max-rows' = '500',
      'sink.buffer-flush.interval' = '5s',
      'sink.max-retries' = '3',
      'sink.parallelism' = '1'
      );