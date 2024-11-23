package com.ddsr.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

/**
 * @author ddsr, created it at 2024/11/23 21:07
 */
public class TTLDurationIncreaseDemo {
    public static void main(String[] args) {

        StateTtlConfig ttlConfigOld = StateTtlConfig
                .newBuilder(Time.minutes(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        ValueStateDescriptor<String> descriptor1 = new ValueStateDescriptor<>("stateName", String.class);
        descriptor1.enableTimeToLive(ttlConfigOld);


        StateTtlConfig ttlConfigNew = StateTtlConfig
                .newBuilder(Time.minutes(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        ValueStateDescriptor<String> descriptor2 = new ValueStateDescriptor<>("stateName", String.class);
        descriptor2.enableTimeToLive(ttlConfigNew);
    }
}
