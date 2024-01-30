package com.ddsr.word.count;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * By default, elements are not transferred on the network one-by-one (which would cause unnecessary network traffic)
 * but are buffered. The size of the buffers (which are actually transferred between machines) can be set in the Flink
 * config files. While this method is good for optimizing throughput, it can cause latency issues when the incoming
 * stream is not fast enough. To control throughput and latency, you can use env.setBufferTimeout(timeoutMillis) on the
 * execution environment (or on individual operators) to set a maximum wait time for the buffers to fill up. After this
 * time, the buffers are sent automatically even if they are not full. The default value for this timeout is 100 ms.
 *
 * <p></p>
 * <p>
 * To maximize throughput, set setBufferTimeout(-1) which will remove the timeout and buffers will only be flushed when
 * they are full. To minimize latency, set the timeout to a value close to 0 (for example 5 or 10 ms). A buffer timeout
 * of 0 should be avoided, because it can cause severe performance degradation.
 *
 * @author ddsr, created it at 2024/1/30 17:35
 */
public class BufferTimeoutDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        int timeoutMillis = 1000;
        env.setBufferTimeout(timeoutMillis);

        SingleOutputStreamOperator<String> ds = env.generateSequence(1, 10).map(i -> "*" + i + "*");
        ds.setBufferTimeout(timeoutMillis).print();

        env.execute();

    }

}
