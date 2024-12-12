package com.ddsr.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ddsr, created it at 2023/10/1 22:46
 */
public class CheckpointConfigDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 开启通用增量changelog，要求checkpoint最大并发数为1，其他参数建议在flink-conf.yml中配置
        env.enableChangelogStateBackend(true);

        // 代码中用到hdfs，需要导入hadoop依赖、指定访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "hive");

        // 检查点配置
        // 1、启用检查点: 默认是barrier对齐的，周期为5s, 精准一次
        // Barrier alignment is only needed for providing exactly once guarantees. If you don’t need this, you can gain
        // some performance by configuring Flink to use CheckpointingMode.AT_LEAST_ONCE, which has the effect of
        // disabling barrier alignment.

        // Exactly once this does not mean that every event will be processed exactly once. Instead, it means that
        // every event will affect the state being managed by Flink exactly once.

        // Exactly Once End-to-end #

        //To achieve exactly once end-to-end, so that every event from the sources affects the sinks exactly once, the
        // following must be true:
        //
        //1. your sources must be replayable, and
        //2. your sinks must be transactional (or idempotent)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // Also specify the checkpointing mode through the CheckpointConfig
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 2、指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/chk");
        // 3、checkpoint的超时时间: 默认10分钟，如果超过10分钟，会自动放弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔，设置了>0,并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 8、enable externalized checkpoints which are retained after job cancellation
        // available at flink version 1.20
        // env.getCheckpointConfig().setExternalizedCheckpointRetention(
        //       ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // 9、enable checkpointing with finished tasks
        Configuration conf = new Configuration();
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env.configure(conf);

        //  开启 非对齐检查点（barrier非对齐）
        // 开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
        // Note that savepoints will always be aligned.

        // Alignment happens only for operators with multiple predecessors (joins) as well as operators with multiple
        // senders (after a stream repartitioning/shuffle). Because of that, dataflows with only embarrassingly
        // parallel streaming operations (map(), flatMap(), filter(), …) actually give exactly once guarantees even
        // in at least once mode.

        // Unaligned Recovery: Operators first recover the in-flight data before starting processing any data from
        // upstream operators in unaligned checkpointing. Aside from that, it performs the same steps as during recovery
        // of aligned checkpoints.
        checkpointConfig.enableUnalignedCheckpoints();
        // 开启非对齐检查点才生效： 默认0，表示一开始就直接用 非对齐的检查点
        // 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数，自动切换成 非对齐检查点（barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));


        env
                .socketTextStream("ora11g", 7777)
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}

/*
 *
 * 1、 Barrier对齐： 一个Task收到所有上游的任务的同一个编号的Barrier，才会对自己本地的状态进行备份
 *        精准一次：在Barrier对齐过程中，Barrier后面的数据阻塞等待（不会越过Barrier）
 *        至少一次：在Barrier对齐过程中，先到的Barrier不会阻塞其后的数据，接着计算
 *
 * 2、 非Barrier对齐： 一个Task在收到第一个Barrier时就开始执行备份，能保证精准一次（Flink1.11 later）
 *        先到的Barrier（Task的输入缓存区）， 此时开始执行本地状态备份，不阻塞其后面的数据，继续计算输出， 越过数据（输入缓冲，输出缓冲）
 *                  ，Barrier被放置Task的输出缓存区末尾（最先传递出去），同时被越过的数据被持久化到状态中
 *        未到的Barrier（未到Task的输入缓存区，可能在上游Task输出缓冲中），在它前面的数据继续计算输出，同时这部分数据被持久化到状态中
 *        最后一个分区Barrier到达时，该Task的状态备份完成
 *
 *
 */