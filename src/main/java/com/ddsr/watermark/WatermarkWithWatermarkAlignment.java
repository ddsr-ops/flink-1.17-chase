package com.ddsr.watermark;

/**
 * Contrary to idle sources, the watermark of such downstream operator (like windowed joins on aggregations) can
 * progress. However, such operator might need to buffer excessive amount of data coming from the fast inputs, as the
 * minimal watermark from all of its inputs is held back by the lagging input. All records emitted by the fast input
 * will hence have to be buffered in the said downstream operator state, which can lead into uncontrollable growth of
 * the operator’s state.
 * <p>
 * In order to address the issue, you can enable watermark alignment, which will make sure no
 * sources/splits/shards/partitions increase their watermarks too far ahead of the rest. You can enable alignment for
 * every source separately
 *
 * @author ddsr, created it at 2024/2/21 21:25
 * @see <a
 * href="https://nightlies.apache.org/flink/flink-docs-release-1
 * .18/docs/dev/datastream/event-time/generating_watermarks/#watermark-alignment">Watermark
 * Alignment</a>
 */
public class WatermarkWithWatermarkAlignment {
}
