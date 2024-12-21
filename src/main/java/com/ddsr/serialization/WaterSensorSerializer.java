package com.ddsr.serialization;

import com.ddsr.bean.WaterSensor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Custom a kryo serializer for WaterSensor
 *
 * @author ddsr, created it at 2024/12/20 16:42
 */
public class WaterSensorSerializer extends Serializer<WaterSensor> {
    @Override
    public void write(Kryo kryo, Output output, WaterSensor waterSensor) {
        output.writeString(waterSensor.getId());
        output.writeLong(waterSensor.getTs());
        output.writeInt(waterSensor.getVc());
    }

    @Override
    public WaterSensor read(Kryo kryo, Input input, Class<WaterSensor> aClass) {
        String id = input.readString();
        long ts = input.readLong();
        int vc = input.readInt();
        return new WaterSensor(id, ts, vc);
    }
}
