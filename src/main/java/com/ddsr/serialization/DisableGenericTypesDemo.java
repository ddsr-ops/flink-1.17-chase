package com.ddsr.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * There are cases when programs may want to explicitly avoid using Kryo(default generic type serializer) as a fallback for
 * generic types. The most common one is wanting to ensure that all types are efficiently serialized either through
 * Flink’s own serializers, or via user-defined custom serializers.
 * <p>
 * The setting below will raise an exception whenever a data type is encountered that would go through Kryo:
 * <p>
 * <code>env.getConfig().disableGenericTypes();</code>
 */
public class DisableGenericTypesDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        // Disable the use of generic types
        config.disableGenericTypes();

        // This will cause an UnsupportedOperationException because CustomNonPojoType is a generic type
        try {
            env.fromElements(
                    new CustomNonPojoType("key1", "value1"),
                    new CustomNonPojoType("key2", "value2")
            ).print();
            env.execute();
        } catch (UnsupportedOperationException e) {
            System.out.println("Caught an exception: " + e.getMessage());
        }

        // This will work fine because Person is a POJO and not a generic type
//    env.fromElements(
//            new Person("Fred", "male"),
//            new Person("Wilma", "female")
//    ).print();
//
//    env.execute();
    }

    static class CustomNonPojoType {
        private final String key;
        private final String value;

        public CustomNonPojoType(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "CustomNonPojoType{key='" + key + "', value='" + value + "'}";
        }
    }
}
