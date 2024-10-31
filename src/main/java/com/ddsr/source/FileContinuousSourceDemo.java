package com.ddsr.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author ddsr, created it at 2024/1/25 22:00
 */
public class FileContinuousSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(1000);

        // Read the files in the input directory continuously
        // readFile api has been deprecated
        // If the monitored files are modified, the corresponding files are re-processed from head to end
//        env.readFile(new TextInputFormat(new Path("input")), "input",
//                FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
//                .print();

        FileSource<String> source =
             FileSource.forRecordStreamFormat(
                           new TextLineInputFormat(), new Path("input"))
             .monitorContinuously(Duration.of(1, SECONDS))
                        .build();


//         Read the files in the input directory continuously, when files are modified, modification should be found
        // This implementation can not find the modification, whether the file is modified or files are added, the
        // cause can not addressed at 2024-10-30 17:26:59
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "fs")
                .print();

        env.execute();


    }
}
