package com.ddsr.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author ddsr, created it at 2023/8/19 19:40
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
