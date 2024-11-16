package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * This generator generates watermarks that are lagging behind processing time
 * by a fixed amount. It assumes that elements arrive in Flink after a bounded delay.
 *
 * @author ddsr, created it at 2024/11/16 19:33
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<WaterSensor> {
    private final long maxTimeLag = 1000L;

    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        // do not need to do anything because we work on processing time
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // emit the watermark as current highest timestamp minus the max time lag
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));

    }
}
