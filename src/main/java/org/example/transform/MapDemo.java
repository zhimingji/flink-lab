package org.example.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;
import org.example.function.MapFunctionImpl;

public class MapDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        //方式一：匿名实现类
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });

        //方式二：Lambda表达式
//        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());

        //方式三：定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunctionImpl());

        map.print();

        env.execute();
    }

}
