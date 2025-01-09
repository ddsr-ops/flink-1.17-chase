package com.ddsr.tranforms;

import com.ddsr.window.WindowCoGroupDemo;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch job using coGroup function, meaning the job is process finite elements. However, using coGroup with window
 * operation means the job aims at processing infinite elements, see {@link WindowCoGroupDemo}.
 *
 * @author ddsr, created it at 2025/1/9 9:34
 */
@SuppressWarnings("Convert2Lambda")
public class BatchCoGroupDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Integer>> ds1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2));

        DataSet<Tuple2<String, Integer>> ds2 = env.fromElements(
                Tuple2.of("a", 3),
                Tuple2.of("c", 4)
        );

        ds1.coGroup(ds2)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first,
                                        Iterable<Tuple2<String, Integer>> second, Collector<String> out) {
                        // Convert Iterable to List for multiple traversals
                        List<Tuple2<String, Integer>> firstList = new ArrayList<>();
                        first.forEach(firstList::add);

                        List<Tuple2<String, Integer>> secondList = new ArrayList<>();
                        second.forEach(secondList::add);

                        // Now you can iterate over firstList and secondList as needed without encountering TraversableOnceException
                        for (Tuple2<String, Integer> f : firstList) {
                            for (Tuple2<String, Integer> s : secondList) {
                                out.collect(f.f0 + " == " + s.f0 + " : " + f.f1 + "<----->" + s.f1);
                            }
                        }
                    }
                })
                .print();
    }
}
