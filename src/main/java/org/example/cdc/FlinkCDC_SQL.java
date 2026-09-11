package org.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FlinkCDC_DataStream {
    public static void main(String[] args) throws Exception {

        //1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表处理环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         * 2.开启检查点，Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
         */
        //2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义为精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //2.2 设置检查点存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:9000/checkpoint");
        //2.3 设置检查点超时时间，默认1分钟
        checkpointConfig.setCheckpointTimeout(60 * 1000L);
        //2.4 同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //2.5 最小等待间隔：上一轮checkpoint结束 到 下一轮checkpoint开始之间的间隔，设置了>0,并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 2.6 取消作业时，checkpoint的数据保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.7 允许checkpoint连接失败的次数: 10次
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 2.8 开启非对齐检查点
        checkpointConfig.enableUnalignedCheckpoints();
        //开启非对齐检查点才生效：默认0，表示一开始就直接用 非对齐检查点
        //如果大于0，一开始用 对齐检查点（Barrier对齐），对齐的时间超过这个参数，自动切换成 非对齐检查点（Barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));
        //2.8 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L),Time.minutes(1L)));

        /**
         * 3.创建Flink CDC mysql 的 source
         */
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("TMcode@0204")
                .databaseList("test", "test_remote")
                .tableList("test.t1")//表名需要带上库名
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // SQL 风格
//        tableEnv.executeSql("create table t1(\n" +
//                "id string,\n" +
//                "name string,\n" +
//                "PRIMARY KEY(id) NOT ENFORCED\n" +
//                ")\n" +
//                "with(\n" +
//                "'connector'='mysql-cdc',\n" +
//                "'hostname'='hadoop1',\n" +
//                "'port'='3306',\n" +
//                "'username'='root',\n" +
//                "'password'='TMcode@0204',\n" +
//                "'database-name'='test',\n" +
//                "'table-name'='t1'\n" +
//                ");");
//
//        Table table = tableEnv.sqlQuery("select * from t1");
//        table.execute().print();

        /**
         * 4. 使用CDC Source从MySQL读取数据
         */
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySqlSource");

        //5.打印输出
        mysqlDS.print();

        //6. 执行任务
        env.execute();
    }
}
