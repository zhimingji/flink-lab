package org.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.ArrayList;
import java.util.List;

/**
 * 案例：针对每种传感器输出最高的3个水位值
 */
public class KeyedListStateDemo {
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
                    //定义ListState状态
                    ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //初始化ListState状态
                        vcListState = getRuntimeContext().getListState(new ListStateDescriptor<>("vcListState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //1.来一条数据，存到List状态里
                        vcListState.add(value.getVc());
                        //2.从list状态拿出来（Iterable）,拷贝到一个list中，排序，只留3个最大的
                        Iterable<Integer> vcListIt = vcListState.get();
                        //2.1拷贝到list中
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            vcList.add(vc);
                        }
                        //2.2对List进行降序排序
                        vcList.sort((x1, x2) -> x2 - x1);
                        //2.3只保留最大的3个（list中的格式一定是连续变大，一超过3就立刻清理即可）
                        if(vcList.size() > 3){
                            vcList.remove(3);
                        }

                        out.collect("传感器ID=" + value.getId() + ",最大的3个水位值=" + vcList.toString());

                        //3.更新List状态
                        vcListState.update(vcList);

                        //list状态的api
//                        vcListState.get();      //取出list状态本组的数据，是一个Iterable
//                        vcListState.add();      //向list状态本组添加一个元素
//                        vcListState.addAll();   //向list状态本组添加多个元素
//                        vcListState.update();   //更新list状态本组数据（覆盖）
//                        vcListState.clear();    //清空list状态本组数据
                    }
                }
                ).print();

        env.execute();
    }
}
