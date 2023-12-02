package com.ddsr.sql.java;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Conversion between datastream and table
 *
 * @author ddsr, created it at 2023/12/2 20:45
 */
public class DataStreamAndTable {
    public static void main(String[] args) throws Exception {
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

        Table filterTable = tableEnv.sqlQuery("select id, ts, vc from sensorTable where id = 's2'");
        Table sumTable = tableEnv.sqlQuery("select id, sum(vc) from sensorTable group by id");

        // Convert table into datastream
        // Append stream
        tableEnv.toDataStream(filterTable, WaterSensor.class).print("Filter");
        // Changelog stream, result need to be updated
        tableEnv.toChangelogStream(sumTable, Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("sum(vc)", DataTypes.BIGINT())
                .build())
                .print("Sum");


        // If DataStreamApi is used in context, execute method is needed, unnecessary otherwise.
        streamEnv.execute();


    }
}
