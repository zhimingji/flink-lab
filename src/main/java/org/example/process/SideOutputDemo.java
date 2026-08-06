package org.example.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                );

        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //使用侧输出流告警
                        if (value.getVc() > 10) {
                            ctx.output(warnTag, "当前水位=" + value.getVc() + "，大于阈值10！！！");
                        }
                        //主流正常发送数据
                        out.collect(value);
                    }
                });

        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");

        env.execute();
    }


}

/**
 * TODO 定时器
 * 1.keyed才有
 * 2.时间时间定时器，通过watermark来触发
 *   watermark = > 注册的时间
 *   注意： watermark = 当前最大事件时间 - 等待时间 - 1ms ,因为 -1ms，所以会推迟一条数据
 *          比如 等待=3s, watermark = 8s -3s -1s = 4999ms,不会触发 5s 的定时器
 *          需要watermark = 8s -3s -1s = 5999ms,才能去触发 5s 的定时器
 *3.在process中获取当前watermark,显示的是上一次的watermark
 *  =>因为process还没接收到这条数据对应生成的新watermark
 */
