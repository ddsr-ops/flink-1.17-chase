package com.ddsr.sql.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 对于Flink这样的流处理框架来说，数据流和表在结构上还是有所区别的。所以使用Table API和SQL需要一个特别的运行时环境，这就是所谓的“表环境”（TableEnvironment）。它主要负责：
 * （1）注册Catalog和表；
 * （2）执行 SQL 查询；
 * （3）注册用户自定义函数（UDF）；
 * （4）DataStream和表之间的转换。
 *
 * @author ddsr, created it at 2023/11/30 18:20
 */
public class TableEnv {
    public static void main(String[] args) {
//        EnvironmentSettings environmentSettings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode() // stream mode
//                .build();
//        TableEnvironment.create(environmentSettings);

        // Alternative way to create table environment for streaming
        // Recommend to use this way
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment);

        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");");

        // SQL API
        Table sourceTable = tableEnv.sqlQuery("select * from source");

        // create temporary view based on query result
        tableEnv.createTemporaryView("sourceTable", sourceTable);

        // SQL API
        TableResult tableResult = tableEnv.executeSql("select * from sourceTable where id > 5");

        // sourceTable only be accessed once, referring to print connector
//        tableResult.print();

        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");");

        // SQL API
        // sourceTable only be accessed once, so print or print connector only output once even if they are used at the same time
//        tableEnv.executeSql("insert into sink select * from sourceTable where id > 5");



        // Table API
        // Recommend to use SQL API
        tableEnv.from("source")
                .where($("id").isGreater(5))
                .select($("id"), $("ts"), $("vc"))
                .executeInsert("sink"); // equal to insertInto("tablePath").execute()


    }
}
