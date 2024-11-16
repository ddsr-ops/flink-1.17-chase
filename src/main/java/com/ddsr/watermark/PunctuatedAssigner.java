package com.ddsr.watermark;

import com.ddsr.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * A punctuated watermark generator will observe the stream of events and emit a watermark whenever it sees a special
 * element that carries watermark information.
 * <p>
 * This is how you can implement a punctuated generator that emits a watermark whenever an event indicates that it
 * carries a certain marker
 * <p>
 * It is possible to generate a watermark on every single event. However, because each watermark causes some computation
 * downstream, an excessive number of watermarks degrades performance.
 *
 * @author ddsr, created it at 2024/11/16 19:38
 */
public class PunctuatedAssigner implements WatermarkGenerator<WaterSensor> {
    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        if (event.getVc() % 3 == 0) {
            output.emitWatermark(new Watermark(event.getTs()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // don't need to do anything because we emit in reaction to events above
    }
}
