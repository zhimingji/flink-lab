package org.example.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        /**
         *
         * flatMap: 一进多出(包括0出)
         *      对于s1的数据，一进一出
         *      对于s2的数据，一进多出
         *      对于s3的数据，一进0出
         *
         *    map怎么控制一进一出？
         *      => 使用return
         *    flatMap怎么控制的一进一出？
         *      => 通过Collector来输出，调用几次就输出几条
         * */
        SingleOutputStreamOperator<String> flatMap = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if (value.getId().equals("s1")) {
                    out.collect(value.getVc().toString());
                } else if (value.getId().equals("s2")) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });


        flatMap.print();

        env.execute();
    }

}
