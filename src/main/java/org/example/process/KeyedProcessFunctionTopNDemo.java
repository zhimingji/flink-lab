package org.example.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
import java.util.List;
import java.util.Map;

public class KeyedProcessFunctionTopNDemo {
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
        //TODO 思路二： 使用KeyedProcessFunction实现
        /**
         * 1.按照vc做keyBy,开窗，分别count
         *      => 增量聚合，计算count
         *      => 全窗口，对计算结果count值封装，带上窗口结束时间的标签
         *          => 为了让同一个窗口时间范围的计算结果到一起去
         *
         * 2.对同一个窗口范围的count值进行处理：排序，取前N个
         *      => 按照windowEnd做KeyBy
         *      => 使用process, 来一条调用一次，需要先存起，分开存hashMap,key=windowEnd,value=List
         *          =>使用定时器，对存起来的结果进行排序，取前N个
         */
        //1.按照vc分组，开窗、聚合（增量计算+全量打标签）
        //开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记windowEnd
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorDS.keyBy(WaterSensor -> WaterSensor.getVc())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new VcCountAggregator(),
                        new WindowResult()
                );

        //2.按照窗口标签（窗口结束时间）keyBy,保证同一个窗口时间范围的结果，到一起去。排序，取TopN
        windowAgg.keyBy(x -> x.f2)
                .process(new TopN(2))
                .print();

        env.execute();
    }

    public static  class  VcCountAggregator implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return 0;
        }
    }

    /**
     * 泛型如下：
     * 第一个：输入类型 = 增量函数的输出
     * 第二个：输出类型 = Tuple3<vc, count, windowEnd>,带上窗口结束时间的标签
     * 第三个：key类型，vc-Integer
     * 第四个：窗口类型
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>, Integer, TimeWindow>{
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            //迭代器里面只有一条数据，next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(key,count,windowEnd));
        }
    }


    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer,Integer,Long>, String> {
        //存不同窗口的统计结果，key=windowEnd,value=list数据
        private Map<Long, List< Tuple3<Integer,Integer,Long>>> dataLisMap;
        //要取的Top数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataLisMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            //进入这个方法，只是一条数据，要排序，得到齐才行===》存起来，不同窗口分开存
            //1.存到HashMap中
            Long windowEnd = value.f2;
            if (dataLisMap.containsKey(windowEnd)) {
                //1.1包含vc,不是vc的第一条没直接添加到List中
                List<Tuple3<Integer, Integer, Long>> dataList = dataLisMap.get(windowEnd);
                dataList.add(value);
            }else {
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(value);
                dataLisMap.put(windowEnd, dataList);
            }

            //2.注册一个定时器，windowEnd + 1ms即可
            //同一个窗口范围，应该同时输出,只不过是一条一条调用processElement方法，只需要延迟1ms即可
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //定时器触发
            Long windowEnd = ctx.getCurrentKey();
            //1.排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataLisMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                   //降序，后减前
                    return o2.f1 - o1.f1;
                }
            });

            //2.取TopN
            StringBuffer outStr = new StringBuffer();
            outStr.append("=========================");
            outStr.append("\n");
            //遍历排序后的list,取出前两个，考虑list可能不够2的情况，==》List中的元素的个数和2取最小值
            for(int i = 0 ; i < Math.min(threshold,dataList.size()); i++){
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top" + (i + 1));
                outStr.append("\n");
                outStr.append("vc=" + vcCount.f0);
                outStr.append("\n");
                outStr.append("data=" + vcCount.f1);
                outStr.append("窗口结束时间=" + vcCount.f2);
                outStr.append("\n");
                outStr.append("=========================");
                outStr.append("\n");
            }

            //用完的List,及时清理，节省资源
            dataList.clear();

            out.collect(outStr.toString());

        }
    }

}
