package com.ddsr.window;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author ddsr, created it at 2023/8/20 17:11
 */
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc())
                .keyBy(WaterSensor::getId)
                // 设置滚动事件时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    // （每个子任务中，数据来一条处理一条，缓存计算结果，窗口结束时，才向下游发送计算结果）
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce方法，之前的结果:"+value1 + ",现在来的数据:"+value2);
                        return new WaterSensor(value1.getId(), System.currentTimeMillis(),value1.getVc()+value2.getVc());
                    }
                })
                .print();

        env.execute();
    }
}


/*
 * In the context of Apache Flink, both `reduce()` and `apply()` methods are used to process elements in a window, but they have different use cases and behaviors:

1. **`reduce()` Method:**

   - **Purpose:** The `reduce()` method is used to incrementally aggregate elements in a window. It takes a `ReduceFunction` that combines two elements of the same type into one element of the same type. This method is typically used for aggregating incoming data streams into a smaller set of data points or metrics.
   - **Efficiency:** It is more efficient for simple aggregations because it can combine elements as they arrive, reducing the memory footprint and improving performance.
   - **Use Case:** Common use cases include summing numbers, finding minimum or maximum values, and other similar aggregations where the result and the input elements are of the same type.

   Example from your codebase (`WindowedStream.java:reduce`):
   ```java
   public <R> SingleOutputStreamOperator<R> reduce(
           ReduceFunction<T> reduceFunction,
           ProcessWindowFunction<T, R, K, W> function,
           TypeInformation<R> resultType) {
       // Implementation details
   }
   ```

2. **`apply()` Method:**

   - **Purpose:** The `apply()` method is more general than `reduce()` and allows for a wider range of operations on the windowed data. It can transform the elements of a window into another type and allows for more complex state handling and transformations within the window.
   - **Flexibility:** It provides more flexibility than `reduce()` as it can emit elements of a different type from the input elements. This method is useful when the transformation or aggregation logic is complex or when the output type is different from the input type.
   - **Use Case:** Use cases include complex aggregations, joining elements, or when the aggregation result needs to be an entirely different object than the elements being aggregated.

The `apply()` method is not explicitly mentioned in the provided code snippets from the `WindowedStream.java` file, but in general, Flink's `WindowedStream` API would use `apply()` for more complex window operations that cannot be covered by `reduce()` due to its limitations in terms of flexibility and output type.

In summary, `reduce()` is optimal for simple, incremental aggregations with the same input and output types, while `apply()` is suited for more complex operations that may involve changing the type of the output or require access to the entire window content.
 */