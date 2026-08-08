package org.example.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SavepointDemo {
    public static void main(String[] args) throws Exception {

        //创建StreamExecutionEnvironment(执行环境)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定Parallelism(并行度)
        env.setParallelism(1);

        //配置Checkpoint(检查点)
        //1、启用检查点：默认是barrier对齐的，周期为5s,精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
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

        //开启非对齐检查点（Barrier非对齐）
        //开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
        checkpointConfig.enableUnalignedCheckpoints();
        //开启非对齐检查点才生效：默认0，表示一开始就直接用 非对齐检查点
        //如果大于0，一开始用 对齐检查点（Barrier对齐），对齐的时间超过这个参数，自动切换成 非对齐检查点（Barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        /**
         * 算子ID可通过uid方法指定算子的唯一标识（服务于程序，对开发人员不可见）
         * 算子通过name可指定算子的名称（服务于开发人员，对开发人员可见）
         * 对于没有设置ID的算子，Flink默认会自动进行设置，所以在重新启动应用后可能会导致ID不同而无法兼容以前的状态。
         * 所以为了方便后续的维护，强烈建议在程序中为每一个算子手动指定ID
         */
        env
                .socketTextStream("hadoop1", 7777)
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                ).uid("flatmap-wc").name("wc-flatmap")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
