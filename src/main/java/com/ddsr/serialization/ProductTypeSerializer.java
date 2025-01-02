package com.ddsr.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Avoid sharing the same TypeSerializerSnapshot class across different serializers #
 * Since schema compatibility checks goes through the serializer snapshots, having multiple serializers returning the
 * same TypeSerializerSnapshot class as their snapshot would complicate the implementation for the
 * TypeSerializerSnapshot#resolveSchemaCompatibility and TypeSerializerSnapshot#restoreSerializer() method.
 * <p>
 * This would also be a bad separation of concerns; a single serializer’s serialization schema, configuration, as well
 * as how to restore it, should be consolidated in its own dedicated TypeSerializerSnapshot class.
 *
 * @author ddsr, created it at 2025/1/2 17:21
 */
public class ProductTypeSerializer extends TypeSerializer<Product> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Product> duplicate() {
        return null;
    }

    @Override
    public Product createInstance() {
        return null;
    }

    @Override
    public Product copy(Product from) {
        return null;
    }

    @Override
    public Product copy(Product from, Product reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(Product record, DataOutputView target) throws IOException {

    }

    @Override
    public Product deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public Product deserialize(Product reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<Product> snapshotConfiguration() {
        return new ProductSerializerSnapshot();
    }
}
