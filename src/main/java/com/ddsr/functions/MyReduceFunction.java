package com.ddsr.functions;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author ddsr, created it at 2023/8/19 15:45
 */
public class MyReduceFunction implements ReduceFunction<WaterSensor> {
    /**
     * Invoke reduce functions after keyBy
     *
     * Output type is equal to input types, engage aggregate function if different
     *
     * When one key with only one record is coming, then not process it, return it directly
     *
     * Value1 holds the state processed before, value2 is the one to come
     *
     */
    @Override
    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
        System.out.println("value1 = " + value1);
        System.out.println("value2 = " + value2);
        return new WaterSensor(value2.getId(), value1.getTs(), value1.getVc() + value2.getVc()  );
    }
}
