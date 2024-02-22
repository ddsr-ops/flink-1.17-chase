package com.ddsr.watermark;

import com.ddsr.partitioner.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Contrary to idle sources, the watermark of such downstream operator (like windowed joins on aggregations) can
 * progress. However, such operator might need to buffer excessive amount of data coming from the fast inputs, as the
 * minimal watermark from all of its inputs is held back by the lagging input. All records emitted by the fast input
 * will hence have to be buffered in the said downstream operator state, which can lead into uncontrollable growth of
 * the operator’s state.
 * <p>
 * In order to address the issue, you can enable watermark alignment, which will make sure no
 * sources/splits/shards/partitions increase their watermarks too far ahead of the rest. You can enable alignment for
 * every source separately
 *
 * todo: https://github.com/1996fanrui/fanrui-learning/blob/3d436c314a876801e53cdef99149705846200330/module-flink/src/main/java/com/dream/flink/kafka/alignment/KafkaAlignmentDemo.java#L44
 * @author ddsr, created it at 2024/2/21 21:25
 * @see <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1
 * .18/docs/dev/datastream/event-time/generating_watermarks/#watermark-alignment">Watermark
 * Alignment</a>
 */
public class WatermarkWithWatermarkAlignment {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        // 自定义分区器：数据%分区数，只输入奇数，都只会去往map的一个子任务
        SingleOutputStreamOperator<Integer> socketDS = env
                .socketTextStream("ora11g", 7777)
                .partitionCustom(new MyPartitioner(), r -> r)
                .map(Integer::parseInt)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r * 1000L)
                                .withIdleness(Duration.ofSeconds(5))  //如果超过5s，上游子任务都没有更新watermark（来数据），则不管这个上游子任务
                                .withWatermarkAlignment("alignment", Duration.ofSeconds(3), Duration.ofSeconds(1))
                );

        SingleOutputStreamOperator<Integer> socketDS1 = env
                .socketTextStream("ora11g", 8888)
                .partitionCustom(new MyPartitioner(), r -> r)
                .map(Integer::parseInt)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r * 1000L)
                                .withIdleness(Duration.ofSeconds(5))  //如果超过5s，上游子任务都没有更新watermark（来数据），则不管这个上游子任务
                                .withWatermarkAlignment("alignment", Duration.ofSeconds(3), Duration.ofSeconds(1))
                );


        // 分成两组： 奇数一组，偶数一组 ， 开10s的事件时间滚动窗口
        socketDS
                .union(socketDS1)
                .keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements);

                    }
                })
                .print();

        env.execute();
    }
}
