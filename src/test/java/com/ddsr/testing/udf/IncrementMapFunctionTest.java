package com.ddsr.testing.udf;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link IncrementMapFunction}
 *
 * @author ddsr, created it at 2024/9/3 6:35
 */
public class IncrementMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        IncrementMapFunction incrementMapFunction = new IncrementMapFunction();

        assertEquals(Optional.of(3L), Optional.ofNullable(incrementMapFunction.map(2L)));

    }
}