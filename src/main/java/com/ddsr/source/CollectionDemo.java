package com.ddsr.source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2023/8/13 16:27
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        // https://blog.csdn.net/java_lifeng/article/details/90413273
        /* Typically, you only need to use getExecutionEnvironment(), since this will do the right thing depending on
         the context: if you are executing your program inside an IDE or as a regular Java program it will create a
         local environment that will execute your program on your local machine. If you created a JAR file from your
         program, and invoke it through the command line, the Flink cluster manager will execute your main method and
          getExecutionEnvironment() will return an execution environment for executing your program on a cluster. */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //        env.fromCollection(Arrays.asList(1, 3, 4, 5))
        //                .print();

        env.fromElements(Integer.class, 1, 3, 4, 5)
                .print();

        // type of elements must be the same
        env.fromElements("3", "3", "5", "5")
                .printToErr();

        // This is synchronous action
//        env.execute();


        // If you don’t want to wait for the job to finish, you can trigger asynchronous job execution by calling
        // executeAsync() on the StreamExecutionEnvironment. It will return a JobClient with which you can
        // communicate with the job you just submitted. For instance, here is how to implement the semantics of
        // execute() by using executeAsync().
        final JobClient jobClient = env.executeAsync();

        final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();

        System.out.println("jobExecutionResult = " + jobExecutionResult);
    }
}
