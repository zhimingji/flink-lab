package org.example.split;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;


/**
 * TODO 使用侧输出流实现分流
 * 需求：  watersensor的数据，s1,s2的数据分别分开
 *
 * 总结步骤：
 *      1、使用process算子
 *      2、定义OutputTag对象
 *      3、调用ctx.output
 *      4、通过主流获取侧流
 *
 * */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop1", 7777).map(new WaterSensorMapFunction());

        /**
         * 创建OutputTag对象
         * 第一个参数： 标签名
         * 第二个参数： 放入侧输出流中的数据的类型，Typeinformation
         * */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1Tag", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2Tag", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getId().equals("s1")) {
                    /**
                     * 如果是s1,放到侧输出流s1中
                     * 上下文调用output,将数据放入侧输出流
                     * 第一个参数：OutputTag对象
                     * 第二个参数：放入侧输出流的数据
                     *
                     * */
                    ctx.output(s1Tag, value);
                } else if (value.getId().equals("s2")) {
                    /**
                     * 如果是s2,放到侧输出流s2中
                     * 上下文调用output,将数据放入侧输出流
                     * 第一个参数：OutputTag对象
                     * 第二个参数：放入侧输出流的数据
                     *
                     * */
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });
        //从主流中，根据标签获取侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);

        //打印主流
        process.print("主流：");

        //打印侧输出流
        s1.print("s1");
        s2.print("s2");

//        s1.printToErr();
//        s2.printToErr();

        env.execute();
    }
}
