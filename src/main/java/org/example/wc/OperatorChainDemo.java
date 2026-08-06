package org.example.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class OperatorChainDemo {
    public static void main(String[] args) throws Exception {
        //todo 1.创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //idea运行也可以看到webui,一般用于本地测试
        //需要引入一个依赖 flink-runtime-web
        //在idea运行，不指定并行度，默认是电脑的线程数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
//        env.disableOperatorChaining();//全局禁用算子链

        //todo 2.读取数据： socket
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);
        //todo 3.处理数据： 切分、转换、分组、聚合

        //Lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
//                .disableChaining()
                .flatMap(
                        (String value, Collector<String> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(word);
                            }
                        }
                        )
                .startNewChain()
//                .disableChaining()
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        //todo 4.输出
        sum.print();
        //todo 5.执行
        env.execute();
    }
}

/**
 1、算子之间的传输关系：
    一对一
    重分区
 2、算子串在一起的条件：
    1）一对一
    2）并行度相同
 3、关于算子链的API:
    1)全局禁用算子链： env.disableOperatorChaining()
    2）某个算子不参与链化： 算子A.disableOperatorChaining(),算子A不会与前面和后面的算子串在一起
    3）从某个算子开启新链条：算子A.startNewChain(),算子A不会与前面的串在一起，从A开始正常链化
 * */
