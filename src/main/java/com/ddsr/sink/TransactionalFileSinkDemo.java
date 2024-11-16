package com.ddsr.sink;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.UUID;

/**
 * beginTransaction – 在事务开始前，我们在目标文件系统的临时目录中创建一个临时文件。随后，我们可以在处理数据时将数据写入此文件。
 * preCommit – 在预提交阶段，我们刷新文件到存储，关闭文件，不再重新写入。我们还将为属于下一个checkpoint的任何后续文件写入启动一个新的事务。
 * commit – 在提交阶段，我们将预提交阶段的文件原子地移动到真正的目标目录。需要注意的是，这会增加输出数据可见性的延迟。
 * abort – 在中止阶段，我们删除临时文件。
 * <p>
 * need more experiments
 * @author ddsr, created it at 2024/11/13 17:29
 */
public class TransactionalFileSinkDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // Create a simple DataStream
        DataStream<String> dataStream = env.fromElements("line1", "line2", "FAIL", "line4");

        // Add a custom transactional file sink
        dataStream.addSink(new CustomTransactionalFileSink("D:\\JavaWorkspaceIJ\\Study\\Flink\\flink-1" +
                ".17-chase\\out\\transactional"));

        // Execute the Flink job
        env.execute("Transactional File Sink Example with Rollback");
    }

    // Custom transactional file sink
    @SuppressWarnings({"ResultOfMethodCallIgnored", "DataFlowIssue"})
    public static class CustomTransactionalFileSink extends TwoPhaseCommitSinkFunction<String, String, Void> {
        private final String outputPath;

        public CustomTransactionalFileSink(String outputPath) {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
            this.outputPath = outputPath;
        }

        @Override
        protected String beginTransaction() {
            String transactionId = UUID.randomUUID().toString();
            String transactionPath = outputPath + "/" + transactionId;
            new File(transactionPath).mkdirs();
            System.out.println("Begin transaction: " + transactionId);
            // return a transaction id
            return transactionId;
        }

        @Override
        protected void invoke(String transaction, String value, Context context) throws Exception {
            /*if ("FAIL".equals(value)) {
                throw new IOException("Simulated failure");
            }*/
            String transactionPath = outputPath + "/" + transaction;
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(transactionPath + "/part-0", true))) {
                writer.write(value);
                writer.newLine();
            }
            System.out.println("Writing to transaction: " + transaction + ", value: " + value);
        }

        @Override
        protected void preCommit(String transaction) {
            System.out.println("Pre-commit transaction: " + transaction);
        }

        @Override
        protected void commit(String transaction) {
            String transactionPath = outputPath + "/" + transaction;
            File transactionDir = new File(transactionPath);
            for (File file : transactionDir.listFiles()) {
                file.renameTo(new File(outputPath + "/" + file.getName()));
            }
            transactionDir.delete();
            System.out.println("Commit transaction: " + transaction);
        }

        @Override
        protected void abort(String transaction) {
            String transactionPath = outputPath + "/" + transaction;
            File transactionDir = new File(transactionPath);
            for (File file : transactionDir.listFiles()) {
                file.delete();
            }
            transactionDir.delete();
            System.out.println("Abort transaction: " + transaction);
        }
    }

}
