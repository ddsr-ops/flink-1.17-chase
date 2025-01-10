package com.ddsr.tranforms;

import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Cache the intermediate result of the transformation. Currently, only jobs that run with batch execution mode are
 * supported. The cache intermediate result is generated lazily at the first time the intermediate result is computed so
 * that the result can be reused by later jobs. If the cache is lost, it will be recomputed using the original
 * transformations.
 *
 * @author ddsr, created it at 2025/1/10 9:20
 */
public class TransCache {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStreamSource<String> dataStream = env.fromElements("1", "2", "3", "4");

        CachedDataStream<String> cachedDataStream = dataStream.cache();

        cachedDataStream.print("The first -> ");

        // invalidate the cache
//        cachedDataStream.invalidate();

        env.execute("The first time to print the cached result");

        cachedDataStream.print("The second -> ");

        // execute again to print the cached result, no anything will be printed if no execution
        env.execute("The second time to print the cached result");

    }
}
