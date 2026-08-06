package org.example.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //2.窗口函数：
        /**
         * 增量聚合Aggregate + 全窗口process
         * 1.增量聚合函数处理数据：来一条计算一条
         * 2.窗口函数触发时，增量聚合的结果（只有一条）传递给全窗口函数
         * 3.经过全窗口函数的处理包装后，输出
         *
         * 结合两者的有点：
         * 1.增量聚合：来一条计算一条，存储中间的计算结果，占用的空间少
         * 2.全窗口函数：可以通过上下文实现
         */
        SingleOutputStreamOperator<String> result = sensorWS.aggregate(
                new MyAgg(),
                new MyProcess()
        );

        result.print();

        env.execute();
    }


    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {

        /**
         * 创建累加器，初始化累加器
         * @return
         */
        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }

        /**
         * 聚合逻辑
         * @param waterSensor
         * @param accumulator
         * @return
         */
        @Override
        public Integer add(WaterSensor waterSensor, Integer accumulator) {
            System.out.println("调用add方法");
            return accumulator + waterSensor.getVc();
        }

        /**
         * 获取最终结果，窗口触发时输出
         * @param accumulator
         * @return
         */
        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }


        @Override
        public Integer merge(Integer integer, Integer acc1) {
            //只有会话窗口才会用到
            System.out.println("调用merge方法");
            return 0;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            //上下文可以拿到window对象，还有其他东西：侧输出流等等
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

            long count = elements.spliterator().estimateSize();

            out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据====》" + elements.toString());
        }
    }
}
