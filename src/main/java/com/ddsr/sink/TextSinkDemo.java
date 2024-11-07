package com.ddsr.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Note that the write*() methods on DataStream are mainly intended for debugging purposes. They are not participating
 * in Flink’s checkpointing, this means these functions usually have at-least-once semantics. The data flushing to the
 * target system depends on the implementation of the OutputFormat. This means that not all elements send to the
 * OutputFormat are immediately showing up in the target system. Also, in failure cases, those records might be lost.
 * <p>
 * For reliable, exactly-once delivery of a stream into a file system, use the FileSink. Also, custom implementations
 * through the .addSink(...) method can participate in Flink’s checkpointing for exactly-once semantics.
 *
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
