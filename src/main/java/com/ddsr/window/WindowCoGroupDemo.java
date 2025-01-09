package com.ddsr.window;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * A demo of window-coGroup function
 * <p><strong>join() method</strong>
 * <p>
 * The join() method is used to perform an inner join between two data streams based on a common key. It produces pairs
 * of elements from both streams that share the same key and fall within a specified time window.
 * <p>
 * Key Characteristics:
 * Inner Join: Only elements with matching keys in both streams are included in the output.
 * <p>
 * Windowed Operation: Requires a time window (e.g., event time, processing time) to define the range of elements to
 * join.
 * <p>
 * Pair Output: The result is a stream of pairs, where each pair contains one element from each input stream that
 * matched the join condition.
 *
 * <p><strong>coGroup() method</strong></p>
 * The coGroup() method is a more general operation that groups elements from two data streams by a common key and
 * allows you to define custom logic for processing the grouped elements. Unlike join(), it does not require a strict
 * one-to-one pairing and can handle cases where elements in one stream do not have a corresponding match in the other
 * stream.
 * <p>
 * Key Characteristics:
 * Grouping Operation: Groups elements from both streams by key but does not enforce a strict pairing.
 * <p>
 * Custom Logic: Allows you to define how grouped elements are processed, making it more flexible than join().
 * <p>
 * Non-Windowed or Windowed: Can be used with or without a time window, depending on the use case.
 *
 * <table border="1">
 *     <caption>Comparison of join() and coGroup()</caption>
 *     <tr>
 *         <th>Feature</th>
 *         <th>join()</th>
 *         <th>coGroup()</th>
 *     </tr>
 *     <tr>
 *         <td>Output</td>
 *         <td>Pairs of matching elements</td>
 *         <td>Custom output based on grouped data</td>
 *     </tr>
 *     <tr>
 *         <td>Join Type</td>
 *         <td>Inner join</td>
 *         <td>Flexible grouping</td>
 *     </tr>
 *     <tr>
 *         <td>Window Requirement</td>
 *         <td>Requires a time window</td>
 *         <td>Can be used with or without a window(in general with a window)</td>
 *     </tr>
 *     <tr>
 *         <td>Use Case</td>
 *         <td>Strict one-to-one matching</td>
 *         <td>Custom logic for grouped elements</td>
 *     </tr>
 *     <tr>
 *         <td>Flexibility</td>
 *         <td>Less flexible</td>
 *         <td>More flexible</td>
 *     </tr>
 * </table>
 */
@SuppressWarnings({"deprecation", "Convert2Lambda"})
public class WindowCoGroupDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create user activity stream with timestamps and watermarks
        DataStream<Tuple2<String, Long>> userActivityStream = env.fromElements(
                new Tuple2<>("user1", 100L),
                new Tuple2<>("user2", 150L),
                new Tuple2<>("user1", 200L),
                new Tuple2<>("user3", 250L),
                new Tuple2<>("user2", 300L)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });

        // Create user registration stream with timestamps and watermarks
        DataStream<Tuple2<String, Long>> userRegistrationStream = env.fromElements(
                new Tuple2<>("user1", 50L),
                new Tuple2<>("user2", 100L),
                new Tuple2<>("user3", 200L),
                new Tuple2<>("user4", 300L) // no activities for this user
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });

        // Co-group the streams
        DataStream<String> result = userActivityStream
                .coGroup(userRegistrationStream)
                .where(key -> key.f0)
                .equalTo(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(
                            Iterable<Tuple2<String, Long>> activities,
                            Iterable<Tuple2<String, Long>> registrations,
                            Collector<String> out) {
                        Long registrationTime = null;
                        Long firstActivityTime;

                        // Collect registration time
                        for (Tuple2<String, Long> reg : registrations) {
                            registrationTime = reg.f1;
                            break; // Assuming one registration per window
                        }

                        // Find the first activity time
                        Long minActivityTime = Long.MAX_VALUE;
                        for (Tuple2<String, Long> act : activities) {
                            if (act.f1 < minActivityTime) {
                                minActivityTime = act.f1;
                            }
                        }
                        firstActivityTime = minActivityTime;

                        // Calculate time difference if both times are available
                        if (registrationTime != null) {
                            long timeDifference = firstActivityTime - registrationTime;
                            out.collect("User: " + registrations.iterator().next().f0 +
                                    ", Time Difference: " + timeDifference + " ms");
                        }
                    }
                });
        result.print();

        env.execute();
    }
}
