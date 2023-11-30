package com.ddsr.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment);

        tableEnv.executeSql("");


    }
}
