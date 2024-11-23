package com.ddsr.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

/**
 * Trying to restore state, which was previously configured without TTL, using TTL enabled descriptor or vice versa
 * will
 * lead to compatibility failure and StateMigrationException.
 * <p>
 * Here's a more detailed breakdown:
 * <li>State Configuration Without TTL: Initially, if you have configured your state without TTL, Flink maintains the
 * state
 * indefinitely without any automatic expiration.</li>
 * <li>State Configuration With TTL: When you configure TTL, Flink adds additional metadata to manage the expiration of
 * state entries based on the TTL settings.</li>
 * <p>
 * When you switch from one configuration to the other, Flink cannot directly understand or reconcile the differences
 * between the state metadata structures. This mismatch leads to a StateMigrationException, indicating that the state
 * cannot be restored due to incompatible configurations.
 *
 * @author ddsr, created it at 2024/11/22 9:18
 */
@SuppressWarnings("unused")
public class TTLVersaWithoutTTL {
    public static void main(String[] args) {
        ValueStateDescriptor<String> descriptorWithoutTTL = new ValueStateDescriptor<>("stateName", String.class);
        // No TTL configuration


        // the Time-to-Live (TTL) settings you configure for state management in Apache Flink do not get stored in
        // the checkpoints or savepoints themselves. Instead, these TTL settings dictate how the state is managed and
        // cleaned up during the execution of the running job.
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        ValueStateDescriptor<String> descriptorWithTTL = new ValueStateDescriptor<>("stateName", String.class);
        descriptorWithTTL.enableTimeToLive(ttlConfig);

        /*
         * If you attempt to restore a Flink job using the second configuration (with TTL) from a savepoint or
         * checkpoint created using the first configuration (without TTL), Flink will throw a StateMigrationException
         *  because the state metadata structures are incompatible.
         */

        // So, how to migrate state?
    }
}
