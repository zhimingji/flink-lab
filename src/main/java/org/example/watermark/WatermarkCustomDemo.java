package org.example.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
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

public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        //默认周期200ms
        env.getConfig().setAutoWatermarkInterval(2000);

        //TODO 1.定义watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                //TODO 指定自定义的生成器
                //1.自定义的周期性生成（匿名实现类写法）
//                .<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
//                    @Override
//                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
//                        return new MyPeriodWatermarkGenerator<>(3000L);
//                    }
//                })
                //2.自定义的断点式生成（Lambda表达式写法）
                .forGenerator(ctx -> new MyPuntuatedWatermarkGenerator<WaterSensor>(3000L))
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
