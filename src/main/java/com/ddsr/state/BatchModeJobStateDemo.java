package com.ddsr.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author ddsr, created it at 2024/11/11 18:26
 */
public class BatchModeJobStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setting state backend will be ignored due to the batch mode
//        env.setStateBackend(...);

        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> ds = env.fromCollection(Arrays.asList(
                new Tuple2<>("hello", 1),
                new Tuple2<>("hello", 1),
                new Tuple2<>("hello", 1),
                new Tuple2<>("world", 1)
        ));

        // In BATCH mode, the configured state backend is ignored. Instead, the input of a keyed operation is grouped
        // by key (using sorting) and then we process all records of a key in turn. This allows keeping only the
        // state of only one key at the same time. State for a given key will be discarded when moving on to the next
        // key.

        ds.keyBy(0).sum(1).print();


        env.execute();
    }
}
