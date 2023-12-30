package com.ddsr.walkthrough;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author ddsr, created it at 2023/12/30 20:24
 */
public class RealTimeReport {
    public static void main(String[] args) {
        // Create the environment settings for streaming mode
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();

        // Create the table environment
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create the transactions table
        tEnv.executeSql("CREATE TABLE transactions (" +
                "    account_id  BIGINT," +
                "    amount      BIGINT," +
                "    transaction_time TIMESTAMP(3)," +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "    'connector' = 'kafka'," +
                "    'topic'     = 'transactions'," +
                "    'properties.bootstrap.servers' = 'kafka:9092'," +
                "    'format'    = 'csv'" +
                ")");

        // Create the spend_report table
        tEnv.executeSql("CREATE TABLE spend_report (" +
                "    account_id BIGINT," +
                "    log_ts     TIMESTAMP(3)," +
                "    amount     BIGINT," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (" +
                "   'connector'  = 'jdbc'," +
                "   'url'        = 'jdbc:mysql://mysql:3306/sql-demo'," +
                "   'table-name' = 'spend_report'," +
                "   'driver'     = 'com.mysql.jdbc.Driver'," +
                "   'username'   = 'sql-demo'," +
                "   'password'   = 'demo-sql'" +
                ")");

        // Get the transactions table
        Table transactions = tEnv.from("transactions");

        // Execute the report function and insert the results into the spend_report table
        report(transactions).executeInsert("spend_report");
    }

    public static Table report(Table transactions) {
        return transactions
                .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts").start().as("log_ts"),
                        $("amount").sum().as("amount"));
    }
}
