package com.ddsr.state.examples.queryable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2024/12/17 9:53
 */
public class CountWindowAverageQueryable {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);


        DataStreamSource<String> socketStream = env.socketTextStream("192.168.20.126", 7777);
        /*
        1,1
        1,3
        1,3
        1,5
         */

        /*
        input: (1,1)
        input: (1,3)
        (2,2)
        input: (1,3)
        input: (1,5)
        (2,4)
         */


        socketStream
                .map(new MapFunction<String, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(String value) {
                        String[] split = value.split(",");
                        return new Tuple2<>(Long.parseLong(split[0]), Long.parseLong(split[1])); // id, metric
                    }
                })
                .keyBy(e -> e.f0) // key by id
                .flatMap(new CountWindowAverage()) // key state after keyBy
                .print();


        env.execute("CountWindowAverageQueryable");
    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum; // a tuple containing the count and the sum

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple2<Long, Long> currentCntSum = sum.value();
            if (currentCntSum != null) {
                currentCntSum.f0 += 1; // count number
                currentCntSum.f1 += input.f1; // sum
            } else{
                currentCntSum = new Tuple2<>(1L, input.f1);
            }

            sum.update(currentCntSum);

            // sout the input
            System.out.println("input: " + input);

            if (currentCntSum.f0 >= 2) {
                out.collect(new Tuple2<>(currentCntSum.f0, currentCntSum.f1 / currentCntSum.f0)); // output count number and average
                sum.clear();
            }
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                            })); // type information
            descriptor.setQueryable("current-cnt-avg");
            sum = getRuntimeContext().getState(descriptor);
        }
    }

}
