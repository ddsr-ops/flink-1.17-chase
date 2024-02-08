package com.ddsr.time;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Processing Time is the wall-clock time on the machine that a record is processed, at the specific instance that the
 * record is being processed. Based on this definition, we see that the results of a computation that is based on
 * processing time are not reproducible. This is because the same record processed twice will have two different
 * timestamps.
 * <p>
 * Despite the above, using processing time in STREAMING mode can be useful. The reason has to do with the fact that
 * streaming pipelines often ingest their unbounded input in real time so there is a correlation between event time and
 * processing time. In addition, because of the above, in STREAMING mode 1h in event time can often be almost 1h in
 * processing time, or wall-clock time. So using processing time can be used for early (incomplete) firings that give
 * hints about the expected results.
 * <p>
 * This correlation does not exist in the batch world where the input dataset is static and known in advance. Given
 * this, in BATCH mode we allow users to request the current processing time and register processing time timers, but,
 * as in the case of Event Time, all the timers are going to fire at the end of the input.
 * <p>
 * Conceptually, we can imagine that processing time does not advance during the execution of a job and we fast-forward
 * to the end of time when the whole input is processed.
 *
 * @author ddsr, created it at 2024/2/4 22:44
 */
public class ProcessTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Take the process time as time characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .print();
        env.execute();
    }
}

