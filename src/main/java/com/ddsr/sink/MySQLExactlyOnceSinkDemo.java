package com.ddsr.sink;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class MySQLExactlyOnceSinkDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);

        YourPojo yourPojo = new YourPojo();
        yourPojo.setCol1("1");
        yourPojo.setCol2(2);
        yourPojo.setCol3(3);
        DataStreamSource<YourPojo> intStream = env.fromCollection(Arrays.asList(yourPojo, yourPojo));

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
                .withMaxRetries(0) // JDBC XA sink requires maxRetries equal to 0, otherwise it could cause duplicates. See issue FLINK-22311 for details.
                .build();

//        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl("jdbc:mysql://your_host:your_port/your_database")
//                .withUsername("your_username")
//                .withPassword("your_password")
//                .build();

        SerializableSupplier<XADataSource> dataSourceSupplier = () -> {
            MysqlXADataSource xaDataSource = new MysqlXADataSource();
            xaDataSource.setURL("jdbc:mysql://localhost:3306/test");
            xaDataSource.setUser("root");
            xaDataSource.setPassword("love8013");

            // Set any additional properties if needed
            // xaDataSource.setProperty("propertyName", "propertyValue");

            return xaDataSource;
        };

        // https://blog.csdn.net/weixin_44990574/article/details/126291239
        SinkFunction<YourPojo> sinkFunction = JdbcSink.exactlyOnceSink(
                sql,
                statementBuilder,
                executionOptions,
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // need to be enabled for MySQL
                        .build(),
                dataSourceSupplier
        );

        intStream.addSink(sinkFunction);

        env.execute();
    }
}

class YourPojo {
    private String col1;
    private int col2;
    private double col3;

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public int getCol2() {
        return col2;
    }

    public void setCol2(int col2) {
        this.col2 = col2;
    }

    public double getCol3() {
        return col3;
    }

    public void setCol3(double col3) {
        this.col3 = col3;
    }

    // Getters and setters
}
