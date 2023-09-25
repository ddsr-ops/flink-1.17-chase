package com.ddsr.split;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/19 19:47
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Integer> ds = env.socketTextStream("192.168.20.126", 7777)
                .map(Integer::valueOf);
        //将ds 分为两个流 ，一个是奇数流，一个是偶数流
        //使用filter 过滤两次
        // one data was process twice , not optimal
        SingleOutputStreamOperator<Integer> ds1 = ds.filter(x -> x % 2 == 0);
        SingleOutputStreamOperator<Integer> ds2 = ds.filter(x -> x % 2 == 1);

        ds1.print("偶数");
        ds2.print("奇数");

        env.execute();
    }
}
