package com.ddsr.combine;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ddsr, created it at 2024/1/7 22:30
 */
public class ConnectKeyCoProcessDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sources for TaxiRide and TaxiFare would be created here
        // This is just a placeholder for the source of TaxiRide events
        DataStream<TaxiRide> rides = env.addSource(/* Your TaxiRide source, e.g., from Kafka */);
        // This is just a placeholder for the source of TaxiFare events
        DataStream<TaxiFare> fares = env.addSource(/* Your TaxiFare source, e.g., from Kafka */);

        // Join the TaxiRide and TaxiFare streams
        DataStream<EnrichedRide> enrichedRides = rides
                .connect(fares)
                .keyBy(new KeySelector<TaxiRide, Long>() {
                    @Override
                    public Long getKey(TaxiRide ride) {
                        return ride.getRideId();
                    }
                }, new KeySelector<TaxiFare, Long>() {
                    @Override
                    public Long getKey(TaxiFare fare) {
                        return fare.getRideId();
                    }
                })
                .process(new EnrichedRideFare());

        // Print the joined stream
        enrichedRides.print();

        // Execute the Flink job
        env.execute("Taxi Ride Fare Join Job");
    }
}
