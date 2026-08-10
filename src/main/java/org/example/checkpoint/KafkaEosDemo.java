package org.example.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;

/**
 * kafka实现精准一次
 */
public class KafkaEosDemo {
    public static void main(String[] args) throws Exception {
        //创建StreamExecutionEnvironment(执行环境)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 1.启用checkpoint（检查点）,设置为EXACTLY_ONCE（精准一次）
        //1、启用检查点：默认是barrier对齐的，周期为5s,精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //2.指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop1:9000/checkpoint");
        //3.取消作业时，保留外部系统的chk-xx目录
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //TODO 2.读取kafka
        KafkaSource<String> kafkaConfig = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop1:9092,hadoop2:9092,hadoop3:9092")//指定kafka节点的地址和端口
                .setGroupId("flinkGroup")//指定消费者组的id
                .setTopics("tm-topic")//指定消费的主题
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定反序列化器，这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();//flink消费kafka的策略


        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConfig, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaSource");


        //TODO 3.写出到kafka
        /**
         * Kafka Sink:
         * 注意：如果要使用精准一次写入kafka,需要满足以下条件，缺一不可：
         * 1、开启checkpoint
         * 2、设置事务前缀
         * 3、设置事务超时时间：checkpoint < 事务超时时间 < max的15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //指定kafka的地址和端口
                .setBootstrapServers("hadoop1:9092")
                //指定序列化器，指定Topic名称、具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("topic_test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //精准一次，开启2PC（两阶段提交）
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //精准一次，必须设置事务的前缀
                .setTransactionalIdPrefix("flink-")
                //精准一次，必须设置事务超时时间：大于checkpoint时间,小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*10*1000+"")
                .build();

        kafkaSource.sinkTo(kafkaSink);

        env.execute();
    }
}
