package com.ddsr.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ddsr, created it at 2024/11/21 11:13
 */

@SuppressWarnings("deprecation")
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        // Note: this is not average for all values, only two
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        // the first field of tuple only is used be as key without other meaning.
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L),
                        Tuple2.of(2L, 3L), Tuple2.of(2L, 5L), Tuple2.of(2L, 8L), Tuple2.of(2L, 4L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();

        // the printed output will be (1,4) and (1,5), (2, 4), (2, 6)

        /*
         * Processing Tuples:
         * First Tuple (1L, 3L):
         * currentSum is (0L, 0L).
         * After updating, currentSum becomes (1L, 3L).
         * Since currentSum.f0 (count) is 1, it does not emit any result.
         * Second Tuple (1L, 5L):
         * currentSum is (1L, 3L).
         * After updating, currentSum becomes (2L, 8L).
         * Since currentSum.f0 (count) is 2, it emits (1, 8/2) = (1, 4) and clears the state.
         * Third Tuple (1L, 7L):
         * currentSum is reset to (0L, 0L).
         * After updating, currentSum becomes (1L, 7L).
         * Since currentSum.f0 (count) is 1, it does not emit any result.
         * Fourth Tuple (1L, 4L):
         * currentSum is (1L, 7L).
         * After updating, currentSum becomes (2L, 11L).
         * Since currentSum.f0 (count) is 2, it emits (1, 11/2) = (1, 5) and clears the state.
         * Fifth Tuple (1L, 2L):
         * currentSum is reset to (0L, 0L).
         * After updating, currentSum becomes (1L, 2L).
         * Since currentSum.f0 (count) is 1, it does not emit any result.
         * Final Output: The two emitted results are (1, 4) and (1, 5).
         */

        env.execute();


    }
}
