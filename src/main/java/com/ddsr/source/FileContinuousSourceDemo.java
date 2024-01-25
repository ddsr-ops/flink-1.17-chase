package com.ddsr.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author ddsr, created it at 2024/1/25 22:00
 */
public class FileContinuousSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read the files in the input directory continuously
        env.readFile(new TextInputFormat(new Path("input")), "input",
                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
                .print();

        env.execute();


    }
}
