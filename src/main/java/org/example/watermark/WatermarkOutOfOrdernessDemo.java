package org.example.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;

public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        /**
         * 演示watermark多并行度下的传递
         * 1.接收到上有多个，取最小
         * 2.往下游多个发送，广播
         */
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        //TODO 1.定义watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                //1.1指定watermark生成：乱序的,等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //1.2指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        //返回的时间戳，单位：毫秒
                        System.out.println("数据=" + element + ",recordTimestamp=" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                });

        //TODO 2.指定watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDSWithWatermark.keyBy(sensor -> sensor.getId())
                //TODO 3.使用 事件时间语义 的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                    /**
                     * 全窗口函数计算逻辑：窗口触发时才会调用一次，统一计算窗口的所有数据
                     * @param s 分组的key
                     * @param context 窗口对象
                     * @param elements 存的数据
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        //上下文可以拿到window对象，还有其他东西：侧输出流等等
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据====》" + elements.toString());

                    }
                }).print();

        env.execute();
    }
}

/**
 * TODO 内置watermark的生成原理
 * 1.都是周期性生成的：默认200ms
 * 2.有序流： watermark = 当前最大的事件时间 - 1ms
 * 3.乱序流： watermark = 当前最大的事件时间 - 延迟时间 - 1ms
 */
