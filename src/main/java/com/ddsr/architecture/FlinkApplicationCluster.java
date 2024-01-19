package com.ddsr.architecture;

/**
 * <strong>Cluster Lifecycle</strong>: a Flink Application Cluster is a dedicated Flink cluster that only executes jobs
 * from one Flink Application and where the main() method runs on the cluster rather than the client. The job submission
 * is a one-step process: you don’t need to start a Flink cluster first and then submit a job to the existing cluster
 * session; instead, you package your application logic and dependencies into a executable job JAR and the cluster entrypoint
 * (ApplicationClusterEntryPoint) is responsible for calling the main() method to extract the JobGraph. This allows you
 * to deploy a Flink Application like any other application on Kubernetes, for example. The lifetime of a Flink
 * Application Cluster is therefore bound to the lifetime of the Flink Application.
 * <p>
 * <strong>Resource Isolation</strong>: in a Flink Application Cluster, the ResourceManager and Dispatcher are scoped to
 * a single Flink Application, which provides a better separation of concerns than the Flink Session Cluster.
 *
 * @author ddsr, created it at 2024/1/19 22:27
 */
public class FlinkApplicationCluster {
}
