package com.ddsr.sql.java;

import org.apache.flink.connector.jdbc.dialect.mysql.MySqlDialect;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * This demo use cdc-apply-dev.sql to illustrate how the upsert action is performed. Within the demo, breakpoints
 * should be added {@link MySqlDialect#getUpsertStatement(String, String[], String[])} to debug.
 * <p>
 *
 * The upsert SQL looks like this :
 * <blockquote><pre>
 * INSERT INTO `t_card_bind`(`id`, `card_no`, `card_status`, `status`, `update_ts`)
 * VALUES (30, '6115000160013102','01', 0, '2024-08-20 09:13:56')
 * ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),
 *                         `card_no`=VALUES(`card_no`),
 *                         `card_status`=VALUES(`card_status`),
 *                         `status`=VALUES(`status`),
 *                         `update_ts`=VALUES(`update_ts`)
 * </pre></blockquote>
 *
 * This demo should be debugged in tft dev environment, which depends on:
 * <li>MySQL 8 source database</li>
 * <li>MySQL 5 target database</li>
 * <li>Apache Kafka Cluster</li>
 * <li>Apache Kafka Connect Cluster with Debezium MySQL plugins</li>
 * <li>An CDC road streams change events of t_card_info table to Apache Kafka topic</li>
 *
 * @author ddsr, created it at 2024-8-20 09:24:29
 */
public class MySQLUpsertDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Source: Kafka topic as the source
        tableEnv.executeSql(
                "CREATE TABLE t_card_info\n" +
                "(\n" +
                "    id               bigint,\n" +
                "    user_id          string,\n" +
                "    uid              string,\n" +
                "    phone            string,\n" +
                "    card_no          string,\n" +
                "    if_main_card     string,\n" +
                "    card_type        string,\n" +
                "    applet_type      string,\n" +
                "    card_alias       string,\n" +
                "    terminal_type    string,\n" +
                "    latest_bind_time bigint,\n" +
                "    seid             string,\n" +
                "    audit_order_no   string,\n" +
                "    user_name        string,\n" +
                "    card_status      string,\n" +
                "    status           string,\n" +
                "    create_by        string,\n" +
                "    create_time      bigint,\n" +
                "    update_by        string,\n" +
                "    update_time      bigint,\n" +
                "    valid_date       string\n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'mysql8_161_2.physical-card.t_card_info',\n" +
                "      'properties.bootstrap.servers' = 'hadoop189:9093',\n" +
                "      'properties.group.id' = 'flink.physical-card.t_card_info5',\n" +
                "      'format' = 'debezium-json',\n" +
                "      'scan.startup.mode' = 'group-offsets',\n" +
                "      'debezium-json.schema-include' = 'false',\n" +
                "      'properties.auto.offset.reset' = 'earliest'\n" +
                "      );");


        // Target: MySQL 5 table as the target
        tableEnv.executeSql("CREATE TABLE t_card_bind\n" +
                "(\n" +
                "    id          bigint,\n" +
                "    card_no     string,\n" +
                "    card_status string,\n" +
                "    status      string,\n" +
                "    update_ts   timestamp,\n" +
                "    primary key (id) not enforced\n" +
                ") WITH (\n" +
                "      'connector' = 'jdbc',\n" +
                "      'url' = 'jdbc:mysql://88.88.16.113:3306/pboc_trade_push?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai&useServerPrepStmts=true&tinyInt1isBit=false&transformedBitIsBoolean=false',\n" +
                "      'table-name' = 't_card_bind',\n" +
                "      'username' = 'root',\n" +
                "      'sink.buffer-flush.max-rows' = '1000',\n" +
                "      'sink.buffer-flush.interval' = '1',\n" +
                "      'password' = 'root');");



        // Delete events also be replayed in the target table
        tableEnv.executeSql("insert into t_card_bind(id, card_no, card_status, status, update_ts)\n" +
                "select id,\n" +
                "       card_no,\n" +
                "       card_status,\n" +
                "       status,\n" +
                "       to_timestamp(from_unixtime(update_time/1000, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as update_ts\n" +
                "from t_card_info;");

    }
}
