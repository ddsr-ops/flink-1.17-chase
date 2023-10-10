package com.ddsr.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/13 16:38
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt"))
                .build();

        // todo: use forBulkFileFormat to build batch source
        FileSource.forBulkFileFormat()
                        .build();

        // <groupId>org.apache.flink</groupId>
        // <artifactId>flink-connector-files</artifactId>

        // Source implementation , WatermarkStrategy , Source name you customize
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fs")
                .print();

        env.execute();
    }
}
