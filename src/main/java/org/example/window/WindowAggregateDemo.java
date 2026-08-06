package org.example.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        //2.窗口函数：增量聚合aggregate
        /**
         * 1.属于本窗口的第一条数据来，创建窗口，创建累加器
         * 2.增量聚合：来一条数据计算一条，调用一次add方法
         * 3.窗口输出时调用一次getResult方法
         * 4.输入、中间累加器、输出、类型可以不一样，非常灵活
         */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                /**
                 * 第一个类型：输入数据的类型
                 * 第二个类型：累加器的类型，存储的中间计算结果的类型
                 * 第三个类型：输出的额类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {

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
                });

        aggregate.print();

        env.execute();
    }
}
