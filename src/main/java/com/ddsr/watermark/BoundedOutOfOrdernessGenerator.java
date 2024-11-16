package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 *
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 * @author ddsr, created it at 2024/11/16 19:25
 */
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<WaterSensor> {
    private final long maxOutOfOrderness = 2000L;

    private long currentMaxTimestamp;

    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // emit the watermark as current highest timestamp minus the out-of-orderness bound
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}
