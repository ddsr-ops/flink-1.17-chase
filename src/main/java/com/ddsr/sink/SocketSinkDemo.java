package com.ddsr.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2024/11/6 22:30
 */
public class SocketSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        // [root@centos7 ~]# nc -lkp 9999
        //hello
        //world
        env.fromElements("hello", "\n", "world")
        // If no "\n", "helloworld" will be output on the nc side
                .writeToSocket(
                        "192.168.20.140",
                        9999,
                        new SimpleStringSchema()
                );

        env.execute();
    }
}
