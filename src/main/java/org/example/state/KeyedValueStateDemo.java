package org.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * 案例：实现连续两个水位值超过10就发出警报
 */
public class KeyedValueStateDemo {
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
                    //TODO 1.定义值状态
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //TODO 2.在open方法中，初始化值状态
                        //状态描述器两个参数：
                        //      第一个参数，起个名字，唯一不重复
                        //      第二个参数，存储的类型
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
//                        lastVcState.value();// 取出状态里的数据
//                        lastVcState.update();// 更新状态里的数据
//                        lastVcState.clear();// 清楚状态里的数据

                        //1.取出上一条数据的水位值(Integer默认值是null，判断）
                        int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        //2.求差值的绝对值，判断是否超过10
                        Integer vc = value.getVc();
                        if (Math.abs(vc - lastVc) > 10) {
                            out.collect("传感器=" + value.getId() +  "==>当前水位值=" + vc + ",与上一条水位值=" + lastVc + "，相差超过10！！！！");
                        }

                        //3.更新状态里的水位值
                        lastVcState.update(vc);
                    }
                }).print();


        env.execute();
    }
}
