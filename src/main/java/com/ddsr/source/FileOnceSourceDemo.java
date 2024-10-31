package com.ddsr.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Read files once
 *
 * @author ddsr, created it at 2024/10/30 17:29
 */
public class FileOnceSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.readFile(
                        new TextInputFormat(new Path("input")),
                        "input",
                        FileProcessingMode.PROCESS_ONCE,
                        1000
                )
                .print();

        env.execute();

    }
}
