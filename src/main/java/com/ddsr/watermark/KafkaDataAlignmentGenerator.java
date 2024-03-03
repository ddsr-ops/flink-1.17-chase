package com.ddsr.watermark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ddsr, created it at 2024/2/28 21:48
 */
public class KafkaDataAlignmentGenerator {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.disableOperatorChaining();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = "CREATE TABLE orders (\n" +
                "  app          INT,\n" +
                "  city_id      INT,\n" +
                "  user_id      STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'datagen',\n" +
                "   'rows-per-second'='2',\n" +
                "   'fields.app.min'='100',\n" +
                "   'fields.app.max'='200',\n" +
                "   'fields.city_id.min'='1',\n" +
                "   'fields.city_id.max'='10',\n" +
                "   'fields.user_id.length'='10'\n" +
                ")";

        String slowTopicDDL = "create table slow_topic\n" +
                "(\n" +
                "  app          INT,\n" +
                "  city_id      INT,\n" +
                "  user_id      STRING,\n" +
                "  topic_type   STRING,\n" +
                "  ts           BIGINT\n" +
                ") \n" +
                "with\n" +
                "(\n" +
//                "    'connector' = 'print'\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'slow-topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format' = 'json'\n" +
                ")";

        String fastTopicDDL = "create table fast_topic\n" +
                "(\n" +
                "  app          INT,\n" +
                "  city_id      INT,\n" +
                "  user_id      STRING,\n" +
                "  topic_type   STRING,\n" +
                "  ts           BIGINT\n" +
                ") \n" +
                "with\n" +
                "(\n" +
//                "    'connector' = 'print'\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'fast-topic',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format' = 'json'\n" +
                ")";

        String insertSlowSQL = "insert into slow_topic\n" +
                "select  app, city_id, user_id, 'slow', UNIX_TIMESTAMP() * 1000 - 10000 \n" +
                "from orders";

        String insertFastSQL = "insert into fast_topic\n" +
                "select  app, city_id, user_id, 'fast', UNIX_TIMESTAMP() * 1000 \n" +
                "from orders";

        System.out.println(sourceDDL);
        System.out.println(slowTopicDDL);
        System.out.println(fastTopicDDL);
        System.out.println(insertSlowSQL);
        System.out.println(insertFastSQL);

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(slowTopicDDL);
        tableEnv.executeSql(fastTopicDDL);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(insertFastSQL);
        statementSet.addInsertSql(insertSlowSQL);
        statementSet.execute();
    }
}
