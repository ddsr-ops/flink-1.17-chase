package com.ddsr.serialization;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * If you register Kryo’s JavaSerializer for your custom type, you may encounter ClassNotFoundExceptions even though
 * your custom type class is included in the submitted user code jar. This is due to a know issue with Kryo’s
 * JavaSerializer, which may incorrectly use the wrong classloader.
 * <p>
 * In this case, you should use org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer instead to resolve the
 * issue. This is a reimplemented JavaSerializer in Flink that makes sure the user code classloader is used.
 *
 * @author ddsr, created it at 2025/1/3 15:40
 */
public class ClassLoaderJavaSerializer {
    public static void main(String[] args) {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().addDefaultKryoSerializer(
                Person.class,
                PersonJavaSerializer.class
        );
    }
}
