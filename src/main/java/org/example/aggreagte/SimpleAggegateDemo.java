package org.example.aggreagte;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;

public class SimpleAggegateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 2L, 1),
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s2", 1L, 22),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyBy = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        /**
         * TODO 简单聚合算子
         * 1、keyBy之后才能调用
         * 2、分组内的聚合：对同一个key内的数据进行聚合
         * */

//        SingleOutputStreamOperator<WaterSensor> result = keyBy.sum("vc");

        /**
         * max/maxBy的区别：
         *      max:只会取比较字段的最大值，非比较字段保留第一次的值
         *      maxBy:取比较字段的最低值，同时非比较字段取最大值这条数据的值
         * */

        //传位置索引的，适用于Tuple类型，POJO类型不行
//        SingleOutputStreamOperator<WaterSensor> result = keyBy.max("vc");
        SingleOutputStreamOperator<WaterSensor> result = keyBy.min("vc");

//        SingleOutputStreamOperator<WaterSensor> result = keyBy.minBy("vc");
//        SingleOutputStreamOperator<WaterSensor> result = keyBy.maxBy("vc");
        result.print();

        env.execute();
    }

}
