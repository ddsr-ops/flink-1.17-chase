package com.ddsr.functions;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author ddsr, created it at 2023/8/19 15:44
 */
public class MyKeySelector implements KeySelector<WaterSensor, String> {
    @Override
    public String getKey(WaterSensor value) throws Exception {
        return value.getId();
    }
}
