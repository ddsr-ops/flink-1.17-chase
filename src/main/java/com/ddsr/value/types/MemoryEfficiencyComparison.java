package com.ddsr.value.types;

import org.apache.flink.types.IntValue;

/**
 * Pre-defined Value types can be more memory-efficient compared to their boxed counterparts (Integer, Double, etc.)
 * because they avoid the overhead of auto-boxing and unboxing.
 *
 * @author ddsr, created it at 2024/12/19 13:57
 */
public class MemoryEfficiencyComparison {
    private static final int NUM_ITERATIONS = 100_000_000; // IntValue wins
//    private static final int NUM_ITERATIONS = 10_000; // Integer wins

    public static void main(String[] args) {
        long startTime, endTime;

        // Benchmark using Integer
        startTime = System.currentTimeMillis();
        benchmarkInteger();
        endTime = System.currentTimeMillis();
        System.out.println("Time taken using Integer: " + (endTime - startTime) + " ms");

        // Benchmark using IntValue
        startTime = System.currentTimeMillis();
        benchmarkIntValue();
        endTime = System.currentTimeMillis();
        System.out.println("Time taken using IntValue: " + (endTime - startTime) + " ms");
    }

    private static void benchmarkInteger() {
        Integer sum = 0;
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            sum += i;
        }
        System.out.println(sum);
    }

    private static void benchmarkIntValue() {
        IntValue sum = new IntValue(0);
        IntValue temp = new IntValue();
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            temp.setValue(i);
            sum.setValue(sum.getValue() + temp.getValue());
        }
        System.out.println(sum.getValue());
    }
}
