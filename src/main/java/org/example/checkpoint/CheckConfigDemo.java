package org.example.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;


public class CheckConfigDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //TODO 最终检查点：Flink1.15开始，默认是ture
//        configuration.set(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,false);

        //代码中用到hdfs,需要带入hadoop的依赖，指定访问hdfs的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");

        //服务器环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //本地环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        //TODO 开启 Changelog
        //要求checkpoint的最大并发数必须为1，其他参数建议在flink-conf配置文件中去指定
        env.enableChangelogStateBackend(true);


        //TODO 检查点配置
        //1、启用检查点：默认是barrier对齐的，周期为5s,精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        /**
         *  检查点存储位置此处踩坑：
         *  此处由于使用部署在云端的集群，因为只有hadoop1开放公网IP，hadoop2和hadoop3均为私网，
         *  因此本地直接访问hdfs只能访问hadoop1，无法正常访问hadoop2和hadoop3,
         *  所以检查点保存一直失败
         */
        //2.指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:9000/checkpoint");
//        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");
        //3.checkpoint的超时时间：默认10分钟
        checkpointConfig.setCheckpointTimeout(60 * 1000);
        //4.同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //5.最小等待间隔：上一轮checkpoint结束 到 下一轮checkpoint开始之间的间隔，设置了>0,并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        //6.取消作业时，checkpoint的数据是否保留在外部系统,企业一般使用RETAIN_ON_CANCELLATION
        //DELETE_ON_CANCELLATION:主动cancel时，删除外部系统的chk-xx目录（如果是程序突然挂掉，不会删）
        //RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //7.允许checkpoint连接失败的次数，默认0，（默认0表示checkpoint一失败，job就挂掉）
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        //TODO 开启非对齐检查点（Barrier非对齐）
        //开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
        checkpointConfig.enableUnalignedCheckpoints();
        //开启非对齐检查点才生效：默认0，表示一开始就直接用 非对齐检查点
        //如果大于0，一开始用 对齐检查点（Barrier对齐），对齐的时间超过这个参数，自动切换成 非对齐检查点（Barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        env
                .socketTextStream("hadoop1", 7777)
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}

/**
 * 1.Barrier对齐: 一个Task收到所有上游同一个编号的barrier之后，才会对自己的本地状态做备份
 *      精准一次: 在barrier对齐过程中，barrier后面的数据阻塞等待（不会越过barrier）
 *      至少一次: 在barrier对齐的过程中，先到的barrier，其后面的数据不阻塞，接着计算
 *
 * 2.非Barrier对齐: 一个Task收到第一个barrier时，就开始执行备份，能保证精准一次（Flink1.11出的新算法）
 *      先到的barrier，将本地状态备份，其后面的数据接着计算输出
 *      未到的barrier，其前面的数据接着计算输出，同时也保存到备份中
 *      最后一个barrier到达该Task时，这个Task的备份结束
 */
