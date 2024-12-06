package com.ddsr.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * More information, please refer to the docs of {@link CheckpointListener}
 *
 * @author ddsr, created it at 2024/12/6 8:51
 */
public class CheckpointListenerDemo implements SourceFunction<String>, CheckpointListener {
    @Override
    public void notifyCheckpointComplete(long checkpointId) {

        // Once the checkpoint has been completed, this method will be called
        // Without guarantee of the notification being received
        System.out.println("Checkpoint id: " + checkpointId + " completed");

    }

    @Override
    public void run(SourceContext<String> ctx) {

    }

    @Override
    public void cancel() {

    }
}
