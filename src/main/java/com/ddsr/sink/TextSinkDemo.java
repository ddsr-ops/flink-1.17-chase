package com.ddsr.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author ddsr, created it at 2024/11/4 22:09
 */
@SuppressWarnings("deprecation")
public class TextSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        List<Object> list = Arrays.asList(new Staff("张三", 30), new Staff("李四", 40), new Staff("王五", 50));

        executionEnvironment
                .fromCollection(list)
                // Writes elements line-wise as Strings. The Strings are obtained by calling the toString() method of
                // each element.
                .writeAsText("D:\\JavaWorkspaceIJ\\Study\\Flink\\flink-1.17-chase\\out\\Staff.txt");


        executionEnvironment.execute();

    }

}
