package com.ddsr.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Use operator list state to sum the number of elements
 *
 * @author ddsr, created it at 2023/9/30 17:42
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .socketTextStream("ora11g", 7777)
                .map(new MyCountMapFunction())
                .print();


        env.execute();
    }


    //  1.实现 CheckpointedFunction 接口
    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;


        @Override
        public Long map(String value) throws Exception {
            return ++count;
        }

        /**
         *  2.本地变量持久化：将 本地变量 拷贝到 算子状态中,开启checkpoint时才会调用
         * 准确地说，在做checkpoint时，才会调用这个方法，将状态数据持久化到状态后端
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            // 2.1 清空算子状态
            state.clear();
            // 2.2 将 本地变量 添加到 算子状态 中
            state.add(count);
        }

        /**
         *  3.初始化本地变量：程序启动和恢复时， 从状态中 把数据添加到 本地变量，每个子任务调用一次
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            // 3.1 从 上下文 初始化 算子状态
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state", Types.LONG));

            // 3.2 从 算子状态中 把数据 拷贝到 本地变量
            if (context.isRestored()) {
                for (Long c : state.get()) { // states are cleared before taking checkpoints, so only one element should be present in the state
                    count += c; // so count += 1, or count = c is the same
                }
            }
        }
    }
}

