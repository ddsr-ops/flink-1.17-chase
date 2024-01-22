package com.ddsr.architecture;

/**
 * <strong>Cluster Lifecycle</strong>: in a Flink Session Cluster, the client connects to a pre-existing, long-running
 * cluster that can accept multiple job submissions. Even after all jobs are finished, the cluster (and the JobManager)
 * will keep running until the session is manually stopped. The lifetime of a Flink Session Cluster is therefore not
 * bound to the lifetime of any Flink Job.
 * <p>
 * <strong>Resource Isolation</strong>: TaskManager sl   ots are allocated by the ResourceManager on job submission and
 * released once the job is finished. Because all jobs are sharing the same cluster, there is some competition for
 * cluster resources — like network bandwidth in the submit-job phase. One limitation of this shared setup is that if
 * one TaskManager crashes, then all jobs that have tasks running on this TaskManager will fail; in a similar way, if
 * some fatal error occurs on the JobManager, it will affect all jobs running in the cluster.
 * <p>
 * <strong>Other considerations</strong>: having a pre-existing cluster saves a considerable amount of time applying for
 * resources and starting TaskManagers. This is important in scenarios where the execution time of jobs is very short
 * and a high startup time would negatively impact the end-to-end user experience — as is the case with interactive
 * analysis of short queries, where it is desirable that jobs can quickly perform computations using existing resources.
 *
 * <p>
 * <i>Formerly, a Flink Session Cluster was also known as a Flink Cluster in session mode.</i>
 *
 * @author ddsr, created it at 2024/1/19 22:30
 */
public class FlinkSessionCluster {
}
