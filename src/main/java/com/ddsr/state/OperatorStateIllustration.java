package com.ddsr.state;

/**
 * Operator State (or non-keyed state) is state that is bound to one parallel operator instance. The Kafka Connector is
 * a good motivating example for the use of Operator State in Flink. Each parallel instance of the Kafka consumer
 * maintains a map of topic partitions and offsets as its Operator State.
 * <p>
 * The Operator State interfaces support redistributing state among parallel operator instances when the parallelism is
 * changed. There are different schemes for doing this redistribution.
 * <p>
 * In a typical stateful Flink Application you don’t need operators state. It is mostly a special type of state that is
 * used in source/sink implementations and scenarios where you don’t have a key by which state can be partitioned.
 * <p>
 * Notes: Operator state is still not supported in Python DataStream API.
 *
 * @author ddsr, created it at 2024/11/29 14:57
 */
@SuppressWarnings("unused")
public class OperatorStateIllustration {
}
