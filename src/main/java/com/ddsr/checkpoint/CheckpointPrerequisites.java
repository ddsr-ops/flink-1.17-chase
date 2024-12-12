package com.ddsr.checkpoint;

/**
 * Flink’s checkpointing mechanism interacts with durable storage for streams and state. In general, it requires:
 *
 * <li>A persistent (or durable) data source that can replay records for a certain amount of time. Examples for such
 * sources are persistent messages queues (e.g., Apache Kafka, RabbitMQ, Amazon Kinesis, Google PubSub) or file systems
 * (e.g., HDFS, S3, GFS, NFS, Ceph, …).</li>
 * <li>A persistent storage for state, typically a distributed filesystem (e.g., HDFS, S3, GFS, NFS, Ceph, …)</li>
 *
 * @author ddsr, created it at 2024/12/12 14:08
 */
@SuppressWarnings("unused")
public class CheckpointPrerequisites {

}
