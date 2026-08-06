package org.example.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;
import org.example.function.FilterFunctionImpl;
import org.example.function.MapFunctionImpl;

public class FilterDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        //方式一：匿名实现类 ,filter:true保留，false过滤掉
//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return value.getId().equals("s1");
//            }
//        });

        //方式二：Lambda表达式
//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(sensor -> sensor.getId().equals("s1"));

        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("s1"));

        filter.print();

        env.execute();
    }

}
