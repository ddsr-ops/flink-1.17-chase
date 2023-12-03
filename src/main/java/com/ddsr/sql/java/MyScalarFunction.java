package com.ddsr.sql.java;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author ddsr, created it at 2023/12/3 20:35
 */
public class MyScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        DataStreamSource<WaterSensor> sensorDs = streamEnv.fromElements(
                new WaterSensor("s1", 1L, 10),
                new WaterSensor("s1", 1L, 5),
                new WaterSensor("s2", 2L, 20),
                new WaterSensor("s3", 3L, 30)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);


        // Convert datastream into table
        Table sensorTable = tableEnv.fromDataStream(sensorDs);
        tableEnv.createTemporaryView("sensorTable", sensorTable);

        // Register function
        tableEnv.createTemporaryFunction("HashFunction", MyHashFunction.class);

        // Use function on field id, then print result
        // SQL API
//        tableEnv.sqlQuery("select id, HashFunction(id) from sensorTable")
//                .execute()
//                .print();


        // Table API
        sensorTable
                .select($("id"), call("HashFunction", $("id")))
                .execute()
                .print();
    }

    public static class MyHashFunction extends ScalarFunction{

        // Accept any type as input, return int
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o){
            return o.hashCode();
        }
    }
}
