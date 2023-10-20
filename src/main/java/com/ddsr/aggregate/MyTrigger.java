package com.ddsr.aggregate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class MyTrigger extends Trigger<Object, Window> {

    private final ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("count", Integer.class);

    @Override
    public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        // Custom logic to determine when to trigger the window based on the incoming element
        // Return TriggerResult.FIRE to trigger the window, or TriggerResult.CONTINUE to continue accumulation

        ValueState<Integer> countState = ctx.getPartitionedState(countStateDescriptor);
        Integer count = countState.value();

        // Example: Trigger the window for every 5 elements
        if (count != null && count >= 5) {
            countState.clear(); // Reset the state for the next window
            return TriggerResult.FIRE;
        } else {
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            countState.update(count); // Update the count in the state
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        // Custom logic to determine when to trigger the window based on processing time
        // Return TriggerResult.FIRE to trigger the window, or TriggerResult.CONTINUE to continue accumulation

        // Example: Trigger the window every 10 seconds
        if (time >= ctx.getCurrentProcessingTime()) {
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        // Custom logic to determine when to trigger the window based on event time
        // Return TriggerResult.FIRE to trigger the window, or TriggerResult.CONTINUE to continue accumulation

        // Example: Trigger the window when event time exceeds the end time of the window
        if (time >= window.maxTimestamp()) {
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
        // Custom logic to clear any state or resources associated with the trigger
        // This method is called when the window is purged or the trigger is removed
        ValueState<Integer> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.clear();
    }
}
