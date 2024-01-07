package com.ddsr.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ddsr, created it at 2024/1/4 22:29
 */
public class WindowFollowWindowDemo {
    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a stream of sensor readings from a socket source
        DataStream<SensorReading> sensorReadings = env.socketTextStream("192.168.20.126", 7777)
                .map(s -> new SensorReading(s.split(",")[0],
                        Long.parseLong(s.split(",")[1]),
                        Double.parseDouble(s.split(",")[2])));

        // Compute minute averages of sensor readings
        DataStream<SensorReading> minuteAverages = sensorReadings
                .keyBy(SensorReading::getSensorId)
                .timeWindow(Time.minutes(1))
                .aggregate(new AverageAggregate());

        // Compute hourly averages of sensor readings
        DataStream<SensorReading> hourlyAverages = minuteAverages
                .keyBy(SensorReading::getSensorId)
                .timeWindow(Time.hours(1))
                .aggregate(new AverageAggregate());

        // Print the hourly averages
        hourlyAverages.print();

        // Execute the job
        env.execute("Multi-Level Aggregation Example");
    }

    public static class AverageAggregate implements AggregateFunction<SensorReading, Tuple3<String, Double, Integer>, SensorReading> {
        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<>("", 0.0, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(SensorReading value, Tuple3<String, Double, Integer> accumulator) {
            return new Tuple3<>(value.sensorId, accumulator.f1 + value.value, accumulator.f2 + 1);
        }

        @Override
        public SensorReading getResult(Tuple3<String, Double, Integer> accumulator) {
            return new SensorReading(accumulator.f0, System.currentTimeMillis(), accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
    public static class SensorReading {
        public String sensorId;
        public long timestamp;
        public double value;

        public SensorReading(String sensorId, long timestamp, double value) {
            this.sensorId = sensorId;
            this.timestamp = timestamp;
            this.value = value;
        }

        public String getSensorId() {
            return sensorId;
        }

        public void setSensorId(String sensorId) {
            this.sensorId = sensorId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }


    }
}
