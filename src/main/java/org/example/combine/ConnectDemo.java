package org.example.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> source1 = env.socketTextStream("hadoop1", 7777).map(x -> Integer.parseInt(x));
        DataStreamSource<String> source2 = env.socketTextStream("hadoop2", 8888);

        /**
         * TODO 使用connect合流
         * 1、一次只能连接两条流
         * 2、流的数据类型可以不一样
         * 3、连接后可以调用map、flatmap、process来处理，但是各处理各的
         *
         * */
        ConnectedStreams<Integer, String> connect = source1.connect(source2);
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流：" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字符流：" + value;
            }
        });

        result.print();

        env.execute();
    }
}
