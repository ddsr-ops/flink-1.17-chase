package com.ddsr.testing.udf;

import com.ddsr.state.StatefulFlatMap;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

/**
 * @see <a href=https://stackoverflow.com/questions/70421139/intellij-cant-find-flink-test-harness-util>resolve dependency</a>
 * @author ddsr, created it at 2024/10/17 18:14
 */
public class StatefulFlatMapTest {
    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMap();

        // wrap user defined function into a the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement(2L, 100L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        testHarness.processWatermark(100L);

        //trigger processing time timers by advancing the processing time of the operator directly
        testHarness.setProcessingTime(100L);

        //retrieve list of emitted records for assertions
//        assertThat(testHarness.getOutput(), containsInExactlyThisOrder(3L));

        //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
        //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
    }
}
