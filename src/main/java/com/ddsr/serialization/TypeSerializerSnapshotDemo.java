package com.ddsr.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A serializer’s snapshot, being the single source of truth for how a registered state was serialized, serves as an
 * entry point to reading state in savepoints. In order to be able to restore and access previous state, the previous
 * state serializer’s snapshot must be able to be restored.
 * <p>
 * Flink restores serializer snapshots by first instantiating the TypeSerializerSnapshot with its classname (written
 * along with the snapshot bytes). Therefore, to avoid being subject to unintended classname changes or instantiation
 * failures, TypeSerializerSnapshot classes should:
 * <p>
 * <li>avoid being implemented as anonymous classes or nested classes,</li>
 * <li>have a public, nullary constructor for instantiation</li>
 *
 * @author ddsr, created it at 2025/1/2 16:33
 */
@SuppressWarnings("all")
public class TypeSerializerSnapshotDemo<T> implements TypeSerializerSnapshot<T> {
    // have a public, nullary constructor for instantiation
    public TypeSerializerSnapshotDemo() {
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {

    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

    }

    @Override
    public TypeSerializer restoreSerializer() {
        return null;
    }

    @Override
    public TypeSerializerSchemaCompatibility resolveSchemaCompatibility(TypeSerializer newSerializer) {
        return null;
    }
}
