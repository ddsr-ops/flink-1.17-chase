package com.ddsr.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ddsr, created it at 2024/12/3 18:02
 */
public class UnionListOperatorState {

    public static class CustomSource implements ParallelSourceFunction<Long>, CheckpointedFunction {
        // ? volatile
        private volatile boolean isRunning = true;
        private transient ListState<Long> unionListState;
        private List<Long> localState = new ArrayList<>();

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            unionListState.clear();
            unionListState.addAll(localState);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "unionListState",
                    Long.class
            );

            unionListState = context.getOperatorStateStore().getUnionListState(descriptor);

            if (context.isRestored()) {
                for (Long l : unionListState.get()) {
                    localState.add(l);
                }
            }
        }

        @Override
        public void run(SourceContext<Long> ctx) throws InterruptedException {
            long count = 0;
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(count);
                    localState.add(count);
                    count++;
                }
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {

            isRunning = false;
        }
    }
}
