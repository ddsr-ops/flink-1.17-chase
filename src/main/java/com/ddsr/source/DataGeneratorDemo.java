package com.ddsr.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/17 22:12
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // If count = 100, divide 100 into two sections: 0-49, 50-99 due to parallelism 2.
        env.setParallelism(2);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<String>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    // aLong from 0 to ... eg. 0, 1, 2, 3, 4,
                    public String map(Long aLong) throws Exception {
                        return "Number: " + aLong;
                    }
                },
                100, // how many records to generate for all parallelisms, 10 : 0, 1, 2, 3 ... 9; 如果要指定最大， Long.MAX_VALUE
                RateLimiterStrategy.perSecond(2), // how many records per second
                Types.STRING // returned type

        );

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(),"dataGenerator")
                .print();

        env.execute();


    }

}
