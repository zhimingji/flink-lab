package org.example.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class ProcessAllWindowTopNDemo {
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

        //案例：最近10秒 = 窗口长度，每5秒输出 = 滑动步长
        //TODO 思路一： 所有数据到一起，用hashmap存， key = vc,value = count值
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))//滑动窗口
                .process(new MyTopNProcessAllWindowFunction())
                .print();

        env.execute();
    }

    public static class MyTopNProcessAllWindowFunction extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow>{
        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            //定义一个HashMap用来存，key = vc, value = count值
            HashMap<Integer, Integer> vcHashMap = new HashMap<>();
            //1.遍历数据，统计各个vc出现的次数
            for(WaterSensor ws : elements){
                Integer vc = ws.getVc();
                if(vcHashMap.containsKey(vc)){
                    //1.1key存在，不是这个key的第一条数据，直接累加
                    vcHashMap.put(vc, vcHashMap.get(vc) + 1);
                } else {
                    //1.2key不存在，初始化
                    vcHashMap.put(vc, 1);
                }
            }

            //2.对count值进行排序:利用List来进行排序
            ArrayList<Tuple2<Integer,Integer>> dataList = new ArrayList<>();
            for(Integer vc : vcHashMap.keySet()){
                dataList.add(Tuple2.of(vc,vcHashMap.get(vc)));
            }
            //对List进行排序，根据count值降序
            dataList.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                   //降序：后减前
                    return o2.f1-o1.f0;
                }
            });
            //3.取出count最大的2个vc
            StringBuffer outStr = new StringBuffer();
            outStr.append("=========================");
            outStr.append("\n");
            //遍历排序后的list,取出前两个，考虑list可能不够2的情况，==》List中的元素的个数和2取最小值
            for(int i = 0 ; i < Math.min(2,dataList.size()); i++){
                Tuple2<Integer, Integer> vcCount = dataList.get(i);
                outStr.append("Top" + (i + 1));
                outStr.append("\n");
                outStr.append("vc=" + vcCount.f0);
                outStr.append("\n");
                outStr.append("data=" + vcCount.f1);
                outStr.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss"));
                outStr.append("\n");
                outStr.append("=========================");
                outStr.append("\n");
            }

            out.collect(outStr.toString());
        }
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
