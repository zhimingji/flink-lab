package org.example.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //必须开启checkpoint,否则 精准一次 无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> sourceDS = env.socketTextStream("hadoop1", 7777);

        /**
         * Kafka Sink:
         * 注意：如果要使用精准一次写入kafka,需要满足以下条件，缺一不可：
         * 1、开启checkpoint(后续介绍)
         * 2、设置事务前缀
         * 3、设置事务超时时间：checkpoint < 事务超时时间 < max的15分钟
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //指定kafka的地址和端口
                .setBootstrapServers("hadoop1:9092")
                //指定序列化器，指定Topic名称、具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("tm-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //写到kafka的一致性级别：精准一次，至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果是精准一次，必须设置事务的前缀
                .setTransactionalIdPrefix("flink-")
                //如果是精准一次，必须设置事务超时时间：大于checkpoint时间,小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*10*1000+"")
                .build();

        sourceDS.sinkTo(kafkaSink);

        env.execute();
    }
}
