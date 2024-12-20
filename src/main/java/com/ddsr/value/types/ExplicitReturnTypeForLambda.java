package com.ddsr.value.types;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2024/12/20 14:00
 */
public class ExplicitReturnTypeForLambda {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);

        intStream.map(
                        value -> new Tuple2<>(value, value)
                )
                /*
                 * could not be determined automatically, due to type erasure. You can give type information hints by
                 * using the returns(...) method on the result of the transformation call, or by letting your function
                 * implement the 'ResultTypeQueryable' interface.
                 */
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {} ))
                .print();

        env.execute();

    }
}
