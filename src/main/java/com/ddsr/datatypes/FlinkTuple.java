package com.ddsr.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;

/**
 * @author ddsr, created it at 2024/1/2 17:03
 */
public class FlinkTuple {
    public static void main(String[] args) {
        // Define a tuple
        Tuple2<String, Integer> nameAge = Tuple2.of("ddsr", 18);

        // Access the tuple
        // Zero index is the first element
        System.out.println(nameAge.f0 +  " " + nameAge.f1);

        // Define a tuple comprised of 8 elements, they are strings, '1', '2', '3', '4', '5', '6', '7', '8'
        Tuple8<String, String, String, String, String, String, String, String> tuple = Tuple8.of("1", "2", "3", "4", "5", "6", "7", "8");

        // Access the 15th element of the tuple
        System.out.println(tuple.f5);

    }
}
