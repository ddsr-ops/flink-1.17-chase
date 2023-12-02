package com.ddsr.sql.java;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

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

    /**
     * Conversion between datastream and table with type conversion
     */
    public static void conversionWithType() {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment);

        // （1）原子类型
        //在Flink中，基础数据类型（Integer、Double、String）和通用数据类型（也就是不可再拆分的数据类型）统一称作“原子类型”。原子类型的DataStream，转换之后就成了只有一列的Table，列字段（field）的数据类型可以由原子类型推断出。另外，还可以在fromDataStream()方法里增加参数，用来重新命名列字段。
        DataStreamSource<Long> longDS = executionEnvironment.fromElements(1L, 2L, 3L);

        // rename column name
        Table numTable = tableEnv.fromDataStream(longDS, Schema.newBuilder().column("myLong", DataTypes.BIGINT()).build());


        // （2）Tuple类型
        // 当原子类型不做重命名时，默认的字段名就是“f0”，容易想到，这其实就是将原子类型看作了一元组Tuple1的处理结果。
        //Table支持Flink中定义的元组类型Tuple，对应在表中字段名默认就是元组中元素的属性名f0、f1、f2...。所有字段都可以被重新排序，也可以提取其中的一部分字段。字段还可以通过调用表达式的as()方法来进行重命名。
        DataStreamSource<Tuple2<Long, Integer>> tupleDs = executionEnvironment.fromElements(Tuple2.of(1L, 1), Tuple2.of(2L, 2), Tuple2.of(3L, 3));

        // 将数据流转换成只包含f1字段的表
        Table table = tableEnv.fromDataStream(tupleDs, $("f1"));

        // 将数据流转换成包含f0和f1字段的表，在表中f0和f1位置交换
        Table table1 = tableEnv.fromDataStream(tupleDs, $("f1"), $("f0"));

        // 将f1字段命名为myInt，f0命名为myLong
        Table table2 = tableEnv.fromDataStream(tupleDs, $("f1").as("myInt"), $("f0").as("myLong"));


        // （3）POJO 类型
        // Define the POJO class
         class MyPojo {
            public String id;
            public Long timestamp;
            public Double value;

             public MyPojo(String id, Long timestamp, Double value) {
                 this.id = id;
                 this.timestamp = timestamp;
                 this.value = value;
             }
        }

        // Create a DataStream of POJO objects
        DataStream<MyPojo> pojoDS = executionEnvironment.fromElements(
            new MyPojo("s1", 1L, 10.0),
            new MyPojo("s1", 2L, 5.0),
            new MyPojo("s2", 3L, 20.0),
            new MyPojo("s3", 4L, 30.0)
        );

        // Convert the DataStream into a Table
        Table pojoTable1 = tableEnv.fromDataStream(pojoDS, $("id"), $("value")); // only get two fields

        // Get id then alias it to myId, and get value then alias it to myValue
        Table pojoTable2 = tableEnv.fromDataStream(pojoDS, $("id").as("myId"), $("value").as("myValue"));


        // （4）Row类型
        //Flink中还定义了一个在关系型表中更加通用的数据类型——行（Row），它是Table中数据的基本组织形式。
        //Row类型也是一种复合类型，它的长度固定，而且无法直接推断出每个字段的类型，所以在使用时必须指明具体的类型信息；我们在创建Table时调用的CREATE语句就会将所有的字段名称和类型指定，这在Flink中被称为表的“模式结构”（Schema）。

    }
}
