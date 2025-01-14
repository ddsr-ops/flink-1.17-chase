package com.ddsr.tranforms;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Operator chain manipulation
 *
 * @author ddsr, created it at 2025/1/14 9:19
 */
public class OperatorChain {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5);

        ds.filter(x -> x % 2 == 0)
                // Begin a new chain, starting with this operator. The two mappers will be chained, and filter will
                // not be chained to the first mapper.
                .map(x -> x * 2)
                .startNewChain()
                .map(x -> "==" + x + "==")
                .print();


        env.execute("Operator Chain Example");
    }
}
