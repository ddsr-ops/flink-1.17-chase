package com.ddsr.sql.java;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ddsr, created it at 2023/12/3 21:28
 */
public class MyTableFunction {
    public static void main(String[] args) {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> stringDs = streamEnv.fromElements("hello java", "hello flink", "hello scala");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        Table line = tableEnv.fromDataStream(stringDs, $("line"));

        tableEnv.createTemporaryView("line", line);

        tableEnv.createTemporaryFunction("split_str", MySplitFunction.class);

        tableEnv
                // Cross join , cartiesian join
//                .sqlQuery("select line, word, length from line, lateral table(split_str(line))")
                // rename column name
                .sqlQuery("select line, word1, length1 from line, lateral table(split_str(line)) as T(word1,length1)")
                .execute()
                .print();

    }

    // Hint type to collect(output)
    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>")) // field name word, length will be used in SQL API
    public static class MySplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            String[] arr = str.split(" ");
            for (String s : arr) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}
