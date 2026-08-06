package org.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;

/**
 * 案例：计算每种传感器的水位和
 */
public class KeyedReducingStateDemo {
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

        sensorDS.keyBy(waterSensor -> waterSensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    ReducingState<Integer> vcSumReducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcSumReducingState = getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<>("vcSumReducingState",
                                        new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                                        Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //来一条数据，添加到reducing状态里
                        vcSumReducingState.add(value.getVc());
                        Integer vcSum = vcSumReducingState.get();
                        out.collect("传感器id为" + value.getId() + ",水位总和为：" + vcSum);

                        //Reducing状态的api
//                        vcSumReducingState.get();   //对本组的Reducing状态，获取结果
//                        vcSumReducingState.add();   //对本组的Reducing状态，添加数据
//                        vcSumReducingState.clear(); //对本组的Reducing状态，清空数据
                    }
                }
                ).print();

        env.execute();
    }
}
