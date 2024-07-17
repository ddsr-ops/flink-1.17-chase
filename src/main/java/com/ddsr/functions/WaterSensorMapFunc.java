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
        // Sensor identifier
        String id = elementList[0];
        // Timestamp
        Long ts = Long.valueOf(elementList[1]);
        // A metric value, meaning a count, or temperature
        Integer vc = Integer.valueOf(elementList[2]);
        return new WaterSensor(id, ts, vc);
    }
}
