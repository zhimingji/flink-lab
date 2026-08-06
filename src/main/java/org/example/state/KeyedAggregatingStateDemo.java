package org.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 案例：计算每种传感器的平均水位
 */
public class KeyedAggregatingStateDemo {
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

                    AggregatingState<Integer, Double> vcAvgAggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcAvgAggregatingState = getRuntimeContext()
                                .getAggregatingState(
                                        new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                        "vcAvgAggregatingState",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                return Tuple2.of(0, 0);
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                return accumulator.f0 * 1D / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                return null;
                                            }
                                        },
                                                Types.TUPLE(Types.INT,Types.INT))
                                );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        //将水位值添加到聚合状态中
                        vcAvgAggregatingState.add(value.getVc());

                        //从聚合状态中获取结果
                        Double vcAvg = vcAvgAggregatingState.get();

                        out.collect("传感器id为" + value.getId() + "平均水位值=" + vcAvg);

                        //AggregatingState状态的api
//                        vcAvgAggregatingState.get();        //对本组数据的聚合状态，获取结果
//                        vcAvgAggregatingState.add();        //对本组数据的聚合状态，添加数据，会自动进行聚合
//                        vcAvgAggregatingState.clear();      //对本组数据的聚合状态，清空数据

                    }
                }
                ).print();

        env.execute();
    }
}
