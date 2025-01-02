package com.ddsr.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
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
 * @author ddsr, created it at 2025/1/2 17:20
 */
@SuppressWarnings("unused")
public class PersonTypeSerializer extends TypeSerializer<Person> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Person> duplicate() {
        return null;
    }

    @Override
    public Person createInstance() {
        return null;
    }

    @Override
    public Person copy(Person from) {
        return null;
    }

    @Override
    public Person copy(Person from, Person reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(Person record, DataOutputView target) {

    }

    @Override
    public Person deserialize(DataInputView source) {
        return null;
    }

    @Override
    public Person deserialize(Person reuse, DataInputView source) {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) {

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
    public TypeSerializerSnapshot<Person> snapshotConfiguration() {
        return new PersonSerializerSnapshot();
    }
}
