package org.example.env;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class EnvDemo {
    public static void main(String[] args) throws Exception {
        //todo 1.创建执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
//                .getExecutionEnvironment();//自动识别是远程集群，还是idea本地环境
                .getExecutionEnvironment(conf);

//                .createLocalEnvironment();
//                .createRemoteEnvironment("hadoop1",8081,"hdfs://hadoop-master:9000/user/hadoop");

        //流批一体：代码api是同一套，可以指定为批，也可以指定为流
        //默认为 STREAMING
        //一般不在代码写死，提交时 参数指定：-Dexcution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //todo 2.读取数据： socket
        DataStreamSource<String> socketDS = env.readTextFile("src/main/java/org/example/input/word.txt");
//        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);
        //todo 3.处理数据： 切分、转换、分组、聚合

        //Lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<String> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(word);
                            }
                        }
                        )
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        //todo 4.输出
        sum.print();
        //todo 5.执行

        env.execute();
        //todo 关于execute总结
        /**
        1.默认env.execute()触发一个flink job
                一个main方法可以调用多个execute,但是没意义，指定到第一个就会阻塞住
        2、env.executeAsync(),异步触发，不阻塞
                一个main方法里面executeAsync个数=生成的flink job数
          3、思考：
                yarn-application集群，提交一次，集群里会有几个flink job?
                =>取决于调用了n个executeAsync()
                =>对应application集群里，会有n个job
                =>对应JobManager当中，会有n个JobMaster
         */
//        env.executeAsync();
//        //别的代码
//        env.executeAsync();
    }
}
