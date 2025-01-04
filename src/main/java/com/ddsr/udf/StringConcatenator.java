package com.ddsr.udf;

import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * A custom acculumator
 *
 * @author ddsr, created it at 2025/1/3 17:14
 */
@SuppressWarnings("all")
public class StringConcatenator implements Accumulator<String, String> {
    // String builder
    private StringBuilder builder = new StringBuilder();
    // Delimiter
    private final String delimiter;

    public StringConcatenator(String delimiter) {
        this.delimiter = delimiter;
    }


    @Override
    public void add(String value) {
        if (builder.length() > 0) {
            builder.append(delimiter);
        }
        builder.append(value);

    }

    @Override
    public String getLocalValue() {
        return builder.toString();
    }

    @Override
    public void resetLocal() {
        builder = new StringBuilder();
    }

    @Override
    public void merge(Accumulator<String, String> other) {
        if(builder.length() > 0 && !other.getLocalValue().isEmpty()) {
            builder.append(delimiter);
        }
        builder.append(other.getLocalValue());
    }

    @Override
    public Accumulator<String, String> clone() {
        StringConcatenator newAcc = new StringConcatenator(delimiter);
        newAcc.builder = new StringBuilder(builder.toString());
        return newAcc;
    }
}
