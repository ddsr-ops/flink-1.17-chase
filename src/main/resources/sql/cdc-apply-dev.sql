CREATE TABLE t_card_info
(
    id               bigint,
    user_id          string,
    uid              string,
    phone            string,
    card_no          string,
    if_main_card     string,
    card_type        string,
    applet_type      string,
    card_alias       string,
    terminal_type    string,
    latest_bind_time bigint,
    seid             string,
    audit_order_no   string,
    user_name        string,
    card_status      string,
    status           string,
    create_by        string,
    create_time      bigint,
    update_by        string,
    update_time      bigint,
    valid_date       string
) WITH (
      'connector' = 'kafka',
      'topic' = 'mysql8_161_2.physical-card.t_card_info',
      'properties.bootstrap.servers' = 'hadoop189:9093',
      'properties.group.id' = 'flink.physical-card.t_card_info5',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'group-offsets',
      'debezium-json.schema-include' = 'false',
      'properties.auto.offset.reset' = 'earliest'
      );

CREATE TABLE t_card_bind
(
    id          bigint,
    card_no     string,
    card_status string,
    status      string,
    update_ts   timestamp,
    primary key (id) not enforced
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://88.88.16.113:3306/pboc_trade_push?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useServerPrepStmts=true&tinyInt1isBit=false&transformedBitIsBoolean=false',
      'table-name' = 't_card_bind',
      'username' = 'root',
      'sink.buffer-flush.max-rows' = '1000',
      'sink.buffer-flush.interval' = '1',
      'password' = 'root');

-- Delete events also be replayed in the target table
insert into t_card_bind(id, card_no, card_status, status, update_ts)
select id,
       card_no,
       card_status,
       status,
       to_timestamp(from_unixtime(update_time/1000, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as update_ts
from t_card_info;