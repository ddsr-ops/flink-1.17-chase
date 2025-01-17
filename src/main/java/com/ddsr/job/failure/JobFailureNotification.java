package com.ddsr.job.failure;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2025/1/17 10:20
 */
public class JobFailureNotification {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // Element zero mocks the application failure
        env.fromElements(0, 1, 2, 3, 4)
                .map(x -> 2 / x)
                .print();

        try {
            JobExecutionResult jobExecutionResult = env.execute();

            System.out.println(jobExecutionResult.getJobExecutionResult());
        } catch (Exception e) {
            jobFailureNotification();
            throw new RuntimeException(e);
        }

        // This line is not reachable if exceptions occurred
        System.out.println("The line after try-catch block");


    }

    private static void jobFailureNotification() {
        // Some logic to handle job failures
        System.out.println("Job failed!");
    }
}
