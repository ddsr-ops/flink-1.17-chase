package com.ddsr.sink;

import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Example {
    public static void main(String[] args) {
        String sql = "INSERT INTO your_table (col1, col2, col3) VALUES (?, ?, ?)";

        JdbcStatementBuilder<YourPojo> statementBuilder = new JdbcStatementBuilder<YourPojo>() {
            @Override
            public void accept(PreparedStatement preparedStatement, YourPojo yourPojo) throws SQLException {
                preparedStatement.setString(1, yourPojo.getCol1());
                preparedStatement.setInt(2, yourPojo.getCol2());
                preparedStatement.setDouble(3, yourPojo.getCol3());
            }
        };

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = JdbcConnectionOptions.builder()
                .withUrl("jdbc:mysql://your_host:your_port/your_database")
                .withUsername("your_username")
                .withPassword("your_password")
                .build();

        SerializableSupplier<XADataSource> dataSourceSupplier = () -> {
            // Create and return your XADataSource here
            return null;
        };

        SinkFunction<YourPojo> sinkFunction = JdbcSink.exactlyOnceSink(
                sql,
                statementBuilder,
                executionOptions,
                JdbcExactlyOnceOptions.defaults(),
                dataSourceSupplier
        );
    }
}

class YourPojo {
    private String col1;
    private int col2;
    private double col3;

    // Getters and setters
}
