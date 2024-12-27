package com.ddsr.state.serialization;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;

/**
 * @author ddsr, created it at 2024/12/27 22:30
 */
@SuppressWarnings("unused")
public class IntSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
    public IntSerializerSnapshot() {
        super(() -> IntSerializer.INSTANCE);
    }
}