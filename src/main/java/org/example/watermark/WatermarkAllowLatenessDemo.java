package org.example.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;

public class WatermarkAllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> sensorDSWithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = sensorDSWithWatermark.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))//允许推迟2秒关窗
                .sideOutputLateData(lateTag) //关窗后的迟到数据，放入侧输出流
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
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据====》" + elements.toString());

                    }
                });

        //打印主流
        process.print();
        //从主流获取侧输出流，打印
        process.getSideOutput(lateTag).printToErr("关窗后的迟到数据");


        env.execute();
    }
}

/**
 * 1.乱序与迟到的区别：
 *      乱序： 数据的顺序乱了，出现时间小的比时间大的晚来
 *      迟到：数据的时间戳 < 当前的watermark
 *
 * 2.乱序、迟到数据的处理：
 *      1）watermark中指定乱序等待的时间
 *      2）如果开窗，窗口允许迟到
 *          =》 推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
 *          =》 关窗后，迟到数据不会被计算
 *      3）关窗后的迟到数据，放入侧输出流
 *
 * 问：如果watermark等待3s,窗口允许迟到2s,问什么不直接watermark等待5s,或窗口允许迟到5s?
 * 答：
 *      =》watermark等待时间不会设太大，因为会影响计算的延迟
 *          如果3s ==> 窗口第一次触发计算和输出， 13s的数据来。 13-3 =10s
 *          如果5s ==> 窗口第一次触发计算和输出，15s的数据来。  15-5=10s
 *      =>窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确
 *          如果只设置 允许迟到5s,那么就会导致频繁重新输出
 *
 *TODO 设置经验
 * 1.watermark等待时间，设置一个不算特别大的，一般是秒级，在乱序和延迟取舍
 * 2.设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
 * 3.极端小部分迟到很久的数据，放到侧输出流，获取之后可以做各种处理
 *
 */
