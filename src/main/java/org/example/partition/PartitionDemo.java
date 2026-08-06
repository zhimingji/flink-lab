package org.example.partition;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);

        //1、随机分区：Random.nextInt(下游算子并行度)
//        socketDS.shuffle().print();

        //2、rebalance轮询： nextChannelToSendTo = （nextChannelToSendTo + 1）% 下游算子并行度
//        socketDS.rebalance().print();

        //3、rescale缩放：实现轮询，局部组队，比rebalance高效
//        socketDS.rescale().print();

        //4、broadcast广播： 发送给下游所有的子任务
        socketDS.broadcast().print();

        //5、global全局： 全部发往第一个子任务
//        socketDS.global().print();

        //6、keyBy: 按指定key取发送，相同key发往同一个子任务
        //7、one-to-one: Forward分区器

        //总结：Flink提供7中分区器+1种自定义

        env.execute();
    }
}
