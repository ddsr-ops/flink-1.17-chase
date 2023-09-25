package com.ddsr.process;

import com.ddsr.bean.WaterSensor;
import com.ddsr.functions.WaterSensorMapFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * Keyed Process Function with Timer
 *
 * 1. Keyed Stream have a timer service
 * 2. Event timer are fired by watermark when watermark >= registered time
 *    注意：watermark = 最大事件时间 - 等待时间 - 1ms
 *         比如 5s的定时器， 等待3s， watermark = 8s - 3s - 1ms = 4999ms， 不会触发5s的定时器
 *         需要至少watermark = 8001ms -3s -1m = 5s, 此时触发5s的定时器
 * 3. 在process中获取当前的watermark, 是上一条数据的watermark， 因为watermark还没接收到这条数据对应生成的新的watermark
 * @author ddsr, created it at 2023/9/3 21:02
 */
public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("ora11g", 7777)
                .map(new WaterSensorMapFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO Process:keyed
        SingleOutputStreamOperator<String> process = sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    /**
                     * 来一条数据调用一次
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //获取当前数据的key
                        String currentKey = ctx.getCurrentKey();

                        // TODO 1.定时器注册
                        TimerService timerService = ctx.timerService();

                        // 1、事件时间的案例
                        Long currentEventTime = ctx.timestamp(); // 数据中提取出来的事件时间
                        timerService.registerEventTimeTimer(5000L); // fired when watermark > 5000, that is at least an element with a timestamp of 8001 ms(because of one subtraction in watermark)
                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentEventTime + ",注册了一个5s的定时器");

                        // 2、处理时间的案例
//                        long currentTs = timerService.currentProcessingTime();
//                        timerService.registerProcessingTimeTimer(currentTs + 5000L); // 注意与registerEventTimeTimer的区别
//                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentTs + ",注册了一个5s后的定时器");


                        // 3、获取 process的 当前watermark
                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("当前数据=" + value + ",当前watermark=" + currentWatermark);



                        // 注册定时器： 处理时间、事件时间
//                        timerService.registerProcessingTimeTimer();
//                        timerService.registerEventTimeTimer();
                        // 删除定时器： 处理时间、事件时间
//                        timerService.deleteEventTimeTimer();
//                        timerService.deleteProcessingTimeTimer();

                        // 获取当前时间进展： 处理时间-当前系统时间，  事件时间-当前watermark
//                        long currentTs = timerService.currentProcessingTime();
//                        long wm = timerService.currentWatermark();
                    }


                    /**
                     * TODO 2.时间进展到定时器注册的时间，调用该方法
                     * TimerService会以键（key）和/或时间戳为标准，对定时器进行去重；也就是说对于每个key和/或时间戳，最多只有一个定时器，如果注册了多次，onTimer()方法也将只被调用一次。
                     *      Registers a timer to be fired when the event time watermark passes the given time.
                     * Timers can internally be scoped to keys and/or windows.
                     *
                     * @param timestamp 当前时间进展，就是定时器被触发时的时间
                     * @param ctx       上下文
                     * @param out       采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();

                        System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
                    }
                }
        );

        process.print();

        env.execute();
    }
}

/**
 * Demonstration Log
 * 当前key=s1,当前时间=1000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s1', ts=1, vc=1},当前watermark=-9223372036854775808 == Long.MIN
 * 当前key=s1,当前时间=3000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s1', ts=3, vc=3},当前watermark=-2001 <== the watermark based on the preceding element : 1s - 3s -1ms = -2001ms
 * 当前key=s2,当前时间=7000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s2', ts=7, vc=7},当前watermark=-1
 * 当前key=s3,当前时间=9000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s3', ts=9, vc=9},当前watermark=3999
 * key=s1现在时间是5000定时器触发 <== the current watermark : 9s -3s -1ms = 5999ms >= 5s, fire, s1 was deduplicated
 * key=s3现在时间是5000定时器触发
 * key=s2现在时间是5000定时器触发
 * 当前key=s4,当前时间=8000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s4', ts=8, vc=8},当前watermark=5999 <== the watermark is not pushed forward, not fire (onTimer)
 * 当前key=s5,当前时间=6000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s5', ts=6, vc=6},当前watermark=5999
 * 当前key=s5,当前时间=4000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s5', ts=4, vc=4},当前watermark=5999
 * 当前key=s5,当前时间=3000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s5', ts=3, vc=3},当前watermark=5999
 * 当前key=s1,当前时间=1000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s1', ts=1, vc=1},当前watermark=5999
 * 当前key=s8,当前时间=12000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s8', ts=12, vc=12},当前watermark=5999
 * key=s4现在时间是5000定时器触发 <== the watermark is pushed forward to 8999ms, fire (onTimer)
 * key=s8现在时间是5000定时器触发
 * key=s1现在时间是5000定时器触发
 * key=s5现在时间是5000定时器触发 <== s5 was deduplicated
 * 当前key=s9,当前时间=10000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s9', ts=10, vc=10},当前watermark=8999 <== the watermark is not pushed forward, not fire (onTimer)
 * 当前key=s3,当前时间=3000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s3', ts=3, vc=3},当前watermark=8999 <== the watermark is not pushed forward, not fire (onTimer)
 * 当前key=s1,当前时间=1000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s1', ts=1, vc=1},当前watermark=8999 <== the watermark is not pushed forward, not fire (onTimer)
 * 当前key=s15,当前时间=20000,注册了一个5s的定时器
 * 当前数据=WaterSensor{id='s15', ts=20, vc=20},当前watermark=8999
 * key=s9现在时间是5000定时器触发 <== the watermark is pushed forward to 16999ms, fire (onTimer)
 * key=s15现在时间是5000定时器触发
 * key=s1现在时间是5000定时器触发
 * key=s3现在时间是5000定时器触发
 *
 *
 *
 *
 */