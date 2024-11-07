package com.ddsr.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Note that the write*() methods on DataStream are mainly intended for debugging purposes. They are not participating
 * in Flink’s checkpointing, this means these functions usually have at-least-once semantics. The data flushing to the
 * target system depends on the implementation of the OutputFormat. This means that not all elements send to the
 * OutputFormat are immediately showing up in the target system. Also, in failure cases, those records might be lost.
 * <p>
 * For reliable, exactly-once delivery of a stream into a file system, use the FileSink. Also, custom implementations
 * through the .addSink(...) method can participate in Flink’s checkpointing for exactly-once semantics.
 *
 * @author ddsr, created it at 2023/8/20 9:33
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每个目录中，都有 并行度个数的 文件在写入
        env.setParallelism(2);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(),
                "data-generator");

        FileSink<String> fileSink = FileSink
                // output file through row format, specify encoder as utf-8
                // public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(..){}
                .<String>forRowFormat(new Path("D:/tmp/flink"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withBucketCheckInterval(1) // Control the frequency of checking withRolloverInterval, the default
                // value is 60 seconds
                // Specify file prefix and suffix
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("ddsr-")
                        .withPartSuffix("log")
                        .build())
                // 按照目录进行分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // File roll strategy
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        // Sets the max time a part file can stay open before having to roll. The frequency at which
                        // this is checked is controlled
                        // by the org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
                        // .RowFormatBuilder.withBucketCheckInterval(long) setting
                        .withRolloverInterval(Duration.ofSeconds(3)) // or
                        .withMaxPartSize(new MemorySize(1024 * 1024)) // or
                        .build()
                ).build();


        dataGen.sinkTo(fileSink);


        env.execute();

    }
}
