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
        env.setParallelism(1);

        env.enableCheckpointing(1000);

        // Read the files in the input directory continuously
        // readFile api has been deprecated
        // If the monitored files are modified, the corresponding files are re-processed from head to end

        // IMPLEMENTATION:
        //
        //Under the hood, Flink splits the file reading process into two sub-tasks, namely directory monitoring and
        // data reading. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a
        // single, non-parallel (parallelism = 1) task, while reading is performed by multiple tasks running in
        // parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring
        // task is to scan the directory (periodically or only once depending on the watchType), find the files to be
        // processed, divide them in splits, and assign these splits to the downstream readers. The readers are the
        // ones who will read the actual data. Each split is read by only one reader, while a reader can read
        // multiple splits, one-by-one.
        //
        //IMPORTANT NOTES:
        //
        //If the watchType is set to FileProcessingMode.PROCESS_CONTINUOUSLY, when a file is modified, its contents
        // are re-processed entirely. This can break the “exactly-once” semantics, as appending data at the end of a
        // file will lead to all its contents being re-processed.
        //
        //If the watchType is set to FileProcessingMode.PROCESS_ONCE, the source scans the path once and exits,
        // without waiting for the readers to finish reading the file contents. Of course the readers will continue
        // reading until all file contents are read. Closing the source leads to no more checkpoints after that point
        // . This may lead to slower recovery after a node failure, as the job will resume reading from the last
        // checkpoint.
        env.readFile(new TextInputFormat(new Path("input")), "input",
                        FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
                .print();

//        FileSource<String> source =
//             FileSource.forRecordStreamFormat(
//                           new TextLineInputFormat(), new Path("input"))
//             .monitorContinuously(Duration.of(1, SECONDS))
//                        .build();


//         Read the files in the input directory continuously, when files are modified, modification should be found
        // This implementation can not find the modification, whether the file is modified or files are added, the
        // cause can not addressed at 2024-10-30 17:26:59
//        env.fromSource(source, WatermarkStrategy.noWatermarks(), "fs")
//                .print();

        env.execute();


    }
}
