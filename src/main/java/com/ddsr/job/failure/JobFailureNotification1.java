package com.ddsr.job.failure;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This demo illustrates that notification should be broadcasted to relevant services if the job fails. This is an
 * infinite datestream demo.
 *
 *
 * @author ddsr, created it at 2025/1/17 10:20
 */
public class JobFailureNotification1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // Three zeros can make the job failed
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // Infinite date stream, zero should be populated to mock an application failure
        /*
           nc -lk 7777
           1
           2
           0 --> mock an application failure, which incurs the job failure
         */
        env.socketTextStream("192.168.20.126", 7777)
                .map(x -> 2 / Long.parseLong(x))
                .print();

        try {
            JobExecutionResult jobExecutionResult = env.execute();

            System.out.println(jobExecutionResult.getJobExecutionResult());
        } catch (Exception e) {
            jobFailureNotification();
            throw new RuntimeException(e);
        }

        // This line is not reachable if exceptions occurred(throw new RuntimeException(e))
        System.out.println("The line after try-catch block");


    }

    private static void jobFailureNotification() {
        // Some logic to handle job failures
        System.out.println("Job failed!");
    }
}
