package com.bean;


import com.ddsr.bean.WaterSensor;
import org.apache.flink.types.PojoTestUtils;

/**
 * You can test whether your class adheres to the POJO requirements via
 * org.apache.flink.types.PojoTestUtils#assertSerializedAsPojo() from the flink-test-utils. If you additionally want to
 * ensure that no field of the POJO will be serialized with Kryo, use assertSerializedAsPojoWithoutKryo() instead.
 *
 * @author ddsr, created it at 2024/12/18 17:22
 */
public class TestEligiblePojo {
    public static void main(String[] args) {

        PojoTestUtils.assertSerializedAsPojo(WaterSensor.class);

        PojoTestUtils.assertSerializedAsPojoWithoutKryo(WaterSensor.class);

    }
}