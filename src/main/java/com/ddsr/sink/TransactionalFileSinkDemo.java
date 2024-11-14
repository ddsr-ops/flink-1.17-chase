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
            //            if ("FAIL".equals(value)) {
            //                throw new IOException("Simulated failure");
            //            }
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
