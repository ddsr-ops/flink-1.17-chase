package com.ddsr.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author ddsr, created it at 2023/8/13 16:27
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.fromCollection(Arrays.asList(1, 3, 4, 5))
//                .print();

        env.fromElements(Integer.class, 1,3,4,5)
                .print();

        // type of elements must be the same
        env.fromElements("3", "3", "5", "5")
                        .printToErr();

        env.execute();
    }
}
