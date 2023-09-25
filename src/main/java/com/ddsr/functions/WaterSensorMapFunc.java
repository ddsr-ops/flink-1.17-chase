package com.ddsr.functions;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author ddsr, created it at 2023/8/19 20:00
 */
public class WaterSensorMapFunc implements MapFunction<String, WaterSensor> {
    // value  - s1,1,1
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] elementList = value.split(",");
        return new WaterSensor(elementList[0], Long.valueOf(elementList[1]), Integer.valueOf(elementList[2]));
    }
}
