package com.ddsr.combine;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author ddsr, created it at 2024/1/7 22:30
 */
public class ConnectKeyCoProcessDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sources for TaxiRide and TaxiFare would be created here
        // This is just a placeholder for the source of TaxiRide events
        DataStream<TaxiRide> rides = env.socketTextStream("192.168.20.126", 7777)
                .map(value -> new TaxiRide(Long.parseLong(value.split(",")[0]),
                        Long.parseLong(value.split(",")[1])));

        // This is just a placeholder for the source of TaxiFare events
        DataStream<TaxiFare> fares = env.socketTextStream("192.168.20.126", 8888)
                .map(value -> new TaxiFare(Long.parseLong(value.split(",")[0]),
                        Long.parseLong(value.split(",")[1]),
                        Float.parseFloat(value.split(",")[2])));

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

    public static class EnrichedRideFare extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, EnrichedRide> {

        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide ride, Context context, Collector<EnrichedRide> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new EnrichedRide(ride, fare));
            } else {
                rideState.update(ride);
                // Set a timer to expire the state in one hour
                context.timerService().registerEventTimeTimer(ride.getEventTime() + 3600000);
            }
        }

        @Override
        public void processElement2(TaxiFare fare, Context context, Collector<EnrichedRide> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new EnrichedRide(ride, fare));
            } else {
                fareState.update(fare);
                // Set a timer to expire the state in one hour
                context.timerService().registerEventTimeTimer(fare.getEventTime() + 3600000);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<EnrichedRide> out) throws IOException {
            // Clear any stale state that hasn't been matched and outputted
            if (rideState.value() != null) {
                rideState.clear();
            }
            if (fareState.value() != null) {
                fareState.clear();
            }
        }
    }

    // TaxiRide represents information about a taxi ride event.
    public static class TaxiRide {
        private final long rideId;
        private final long eventTime; // Event time of the ride

        public TaxiRide(long rideId, long eventTime) {
            this.rideId = rideId;
            this.eventTime = eventTime;
        }

        // Other fields, constructors, getters and setters would be here.

        public long getRideId() {
            return rideId;
        }

        public long getEventTime() {
            return eventTime;
        }

        // ... other getters/setters and methods
    }

    // TaxiFare represents information about a taxi fare event.
    public static class TaxiFare {
        private final long rideId;
        private final long eventTime; // Event time when the fare was recorded
        private final float totalFare; // Total fare of the ride

        public TaxiFare(long rideId, long eventTime, float totalFare) {
            this.rideId = rideId;
            this.eventTime = eventTime;
            this.totalFare = totalFare;
        }

        // Other fields, constructors, getters and setters would be here.

        public long getRideId() {
            return rideId;
        }

        public long getEventTime() {
            return eventTime;
        }

        public float getTotalFare() {
            return totalFare;
        }

        // ... other getters/setters and methods
    }

    // EnrichedRide is a combined entity of TaxiRide and TaxiFare.
    public static class EnrichedRide {
        private final TaxiRide ride;
        private final TaxiFare fare;

        // Constructor to combine TaxiRide and TaxiFare into an EnrichedRide.
        public EnrichedRide(TaxiRide ride, TaxiFare fare) {
            this.ride = ride;
            this.fare = fare;
        }

        // Getters and setters for ride and fare would be here.

        public TaxiRide getRide() {
            return ride;
        }

        public TaxiFare getFare() {
            return fare;
        }

        // ... other getters/setters and methods
    }
}
