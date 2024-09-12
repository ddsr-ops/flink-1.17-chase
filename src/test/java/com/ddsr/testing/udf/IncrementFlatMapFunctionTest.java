package com.ddsr.testing.udf;

import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 *
 * The following dependency should be added in the pom.xml
 * <blockquote><pre>
 *         <dependency>
 *             <groupId>org.mockito</groupId>
 *             <artifactId>mockito-core</artifactId>
 *             <version>3.4.6</version> <!-- the version should be consistent with the version of corresponding mockito dependency in flink-test-utils -->
 *             <scope>test</scope>
 *         </dependency>
 * </pre></blockquote>
 *
 * @author ddsr, created it at 2024/9/9 6:47
 */
public class IncrementFlatMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        IncrementFlatMapFunction incrementFlatMapFunction = new IncrementFlatMapFunction();

        Collector<Long> collector = mock(Collector.class);

        // call the methods that have been implemented
        incrementFlatMapFunction.flatMap(2L, collector);

        // verify collector was called with the right output
        // times(1) means that the method should be called once
        Mockito.verify(collector, times(1)).collect(3L);
    }

}
