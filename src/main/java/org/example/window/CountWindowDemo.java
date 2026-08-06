package org.example.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS = sensorKS
//                .countWindow(5);//滚动窗口，窗口长度为5条数据
                .countWindow(5,2);//滑动窗口，窗口长度为5条数据，滚动步长为2条数据（每经过一个步长，都有一个窗口触发输出，第一次输出在第2条数据来的时候）

        SingleOutputStreamOperator<String> process = sensorWS
                .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    /**
                     * 全窗口函数计算逻辑：窗口触发时才会调用一次，统一计算窗口的所有数据
                     * @param s 分组的key
                     * @param context context 窗口对象
                     * @param elements elements 存的数据
                     * @param out out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口包含" + count + "条数据====》" + elements.toString());

                    }
                });

//                    @Override
//                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
//                        //上下文可以拿到window对象，还有其他东西：侧输出流等等
//                        long start = context.window().getStart();
//                        long end = context.window().getEnd();
//                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
//                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
//
//                        long count = elements.spliterator().estimateSize();
//
//                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据====》" + elements.toString());
//
//                    }
//                });

        process.print();

        env.execute();
    }
}
