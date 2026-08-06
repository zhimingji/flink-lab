package org.example.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        //todo 1.创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //idea运行也可以看到webui,一般用于本地测试
        //需要引入一个依赖 flink-runtime-web
        //在idea运行，不指定并行度，默认是电脑的线程数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);

        //todo 2.读取数据： socket
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);
        //todo 3.处理数据： 切分、转换、分组、聚合

        //java表达式
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] words = value.split(" ");
//                for (String word : words) {
//                    out.collect(Tuple2.of(word, 1));
//                }
//            }
//        });

//
//        KeyedStream<Tuple2<String, Integer>, String> wordOneKS = wordOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordOneKS.sum(1);

        //Lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                        ).setParallelism(2)
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
 * 并行度优先级
 * 代码：算子 > 代码：env > 提交时指定 > 配置文件
 * */
