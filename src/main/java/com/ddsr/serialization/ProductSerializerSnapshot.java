package com.ddsr.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Avoid sharing the same TypeSerializerSnapshot class across different serializers #
 * Since schema compatibility checks goes through the serializer snapshots, having multiple serializers returning the
 * same TypeSerializerSnapshot class as their snapshot would complicate the implementation for the
 * TypeSerializerSnapshot#resolveSchemaCompatibility and TypeSerializerSnapshot#restoreSerializer() method.
 * <p>
 * This would also be a bad separation of concerns; a single serializer’s serialization schema, configuration, as well
 * as how to restore it, should be consolidated in its own dedicated TypeSerializerSnapshot class.
 *
 * @see PersonSerializerSnapshot
 * @author ddsr, created it at 2025/1/2 17:15
 */
public class ProductSerializerSnapshot implements TypeSerializerSnapshot<Product> {
    public ProductSerializerSnapshot() {
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {

    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {

    }

    @Override
    public TypeSerializer<Product> restoreSerializer() {
        return null;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Product> resolveSchemaCompatibility(TypeSerializer<Product> newSerializer) {
        return null;
    }
}
