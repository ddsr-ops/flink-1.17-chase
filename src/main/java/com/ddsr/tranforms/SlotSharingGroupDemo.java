package com.ddsr.tranforms;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("Convert2Lambda")
public class SlotSharingGroupDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // Sample source of transactions (id, amount)
        DataStream<Tuple2<String, Double>> transactions = env.fromElements(
                Tuple2.of("TX1", 100.0),
                Tuple2.of("TX2", 200.0),
                Tuple2.of("TX3", 300.0),
                Tuple2.of("TX4", -50.0) // Invalid transaction
        );

        // Filter out invalid transactions
        DataStream<Tuple2<String, Double>> validTransactions = transactions
                .filter((FilterFunction<Tuple2<String, Double>>) transaction -> transaction.f1 > 0)
                .name("FilterInvalidTransactions")
                .slotSharingGroup("ValidationGroup");

        // Calculate statistics on valid transactions
        DataStream<String> statistics = validTransactions
                .map(new MapFunction<Tuple2<String, Double>, String>() {
                    @Override
                    public String map(Tuple2<String, Double> transaction) {
                        return "Statistics: Transaction " + transaction.f0 + " processed.";
                    }
                })
                .name("CalculateStatistics")
                .slotSharingGroup("StatisticsGroup");

        // Alert on transactions above a certain threshold
        DataStream<String> alerts = validTransactions
                .filter(new FilterFunction<Tuple2<String, Double>>() {
                    @Override
                    public boolean filter(Tuple2<String, Double> transaction) {
                        return transaction.f1 > 250.0; // Threshold
                    }
                })
                .map(new MapFunction<Tuple2<String, Double>, String>() {
                    @Override
                    public String map(Tuple2<String, Double> transaction) {
                        return "Alert: High transaction " + transaction.f0 + "!";
                    }
                })
                .name("GenerateAlerts")
                .slotSharingGroup("AlertGroup");

        // Print the results
        transactions.print();
        validTransactions.print();
        statistics.print();
        alerts.print();
        // todo: why no anything output to console?

        // Execute the job
        env.execute("SlotSharingGroupExample");
    }
}