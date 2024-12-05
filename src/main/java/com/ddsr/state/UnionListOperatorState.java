package com.ddsr.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ddsr, created it at 2024/12/3 18:02
 */
public class UnionListOperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        env.addSource(new CustomSource())
                .addSink(new AggregatingSink());

        env.execute("Union List State Aggregation Example");
    }

    public static class CustomSource implements ParallelSourceFunction<Long>, CheckpointedFunction {
        // volatile keyword illustration:
        // the volatile is used to ensure the variable is updated immediately when we change it in other threads
        // the visibility of variable is not guaranteed in java memory model without volatile or synchronized
        private volatile boolean isRunning = true;
        private transient ListState<Long> unionListState;
        private final List<Long> localList = new ArrayList<>();

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            unionListState.clear();
            unionListState.addAll(localList);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                    "unionListState",
                    Long.class
            );

            unionListState = context.getOperatorStateStore().getUnionListState(descriptor);

            // if the job is restored, the recover the state to populate the local list
            if (context.isRestored()) {
                for (Long l : unionListState.get()) {
                    localList.add(l);
                }
            }
        }

        @Override
        public void run(SourceContext<Long> ctx) throws InterruptedException {
            long count = 0;
            while (isRunning) {
                // why to use the synchronized keyword and lock is to reference to the doc of
                // org.apache.flink.streaming.api.functions.source.SourceFunction.run
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(count);
                    // atomic modification of the local list is controlled by checkpoint lock?
                    localList.add(count);
                    // get the thread id, then print
                    System.out.println(Thread.currentThread().getId() + " source count: " + count);
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

    public static class AggregatingSink extends RichSinkFunction<Long> implements CheckpointedFunction {
        private transient ListState<Long> unionListState;
        private final List<Long> localState = new ArrayList<>();

        @Override
        public void close() {
            long sum = 0;
            for (Long l : localState) {
                sum += l;
            }
            System.out.println("sum: " + sum);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            unionListState.clear();
            for (Long l : localState) {
                unionListState.add(l);
            }

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
        public void invoke(Long value, Context context) throws Exception {
            localState.add(value);;
            // get the thread id, then print
            System.out.println(Thread.currentThread().getId() + " sink count: " + value);
        }
    }
}
