package com.ddsr.partitions;

import com.ddsr.partitioner.MyPartitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/19 17:38
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);

        // Random but uniform distributed (均匀分布)
        //        stream.shuffle().print();
//
        // round - robin
        // 如果是数据源倾斜的场景， 在读取数据源后调取rebalance后可解决该问题
//        stream.rebalance().print();

        // 实现轮询， 局部组队内轮询， 比rebalance更高效
//        stream.rescale().print();

        // 子任务都会收到数据源中的同一数据
//        stream.broadcast().print();

        // all data go to the first task
//        stream.global().print();

        // KeyBy ,  data with the same key go to the same sub-task

        // Forward partitioner :  one to one

        // 自定义分区器 CustomPartitionerWrapper
        stream.partitionCustom(new MyPartitioner(), e -> e)
                .print();

        // 7个原生分区器， 1个自定义分区器



        env.execute();
    }
}
