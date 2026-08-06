package org.example.window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //TODO 1.指定窗口分配器：指定用哪一种窗口---时间or计数？ 滚动。滑动、会话？
        //1.1没有keyBy的窗口：窗口内的所有数据进入同一个子任务，并行度只能为1
        //sensorDS.windowAll()
        //1.2有keyBy的窗口： 每个key上都定义了一组窗口，各自独立地进行计算
        //基于时间的
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))//滚动窗口，窗口长度10s
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))//滑动窗口，窗口长度10s，滑动不长2s
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//会话窗口，超时间隔5s
        //基于计数的
//        sensorKS.countWindow(5)//滚动窗口，窗口长度=5个元素
//        sensorKS.countWindow(5,2)//滑动窗口，窗口长度=5个元素，滑动不长=2个元素
//        sensorKS.window(GlobalWindows.create())//全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用

        //TODO 2.指定窗口函数：窗口内数据的计算逻辑


        env.execute();
    }
}
