package org.example.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);


        //匿名表达式写法
        socketDS.partitionCustom(new MyPartitioner(),new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).print();


        //Lambda表达式
        //socketDS.partitionCustom(new MyPartitioner(),r -> r).print();

        env.execute();
    }
}
