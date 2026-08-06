package org.example.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //必须开启checkpoint,否则 精准一次 无法写入kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> sourceDS = env.socketTextStream("hadoop1", 7777);

        /**
         * 如果要指定写入kafka的key
         * 可以自定义反序列化器
         * 1、可以实现一个接口，重写序列化方法
         * 2、指定key，转成字节数组
         * 3、指定value,转成字节数组
         * 4、返回一个ProducerRecord对象，把key,value放进去
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchema<String>() {
                                         @Nullable
                                         @Override
                                         public ProducerRecord<byte[], byte[]> serialize(String s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                             String[] datas = s.split(",");
                                             byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                             byte[] value = datas[1].getBytes(StandardCharsets.UTF_8);
                                             return new ProducerRecord<>("ws",key,value);
                                         }
                                     }
                )

                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*10*1000+"")
                .build();

        sourceDS.sinkTo(kafkaSink);

        env.execute();
    }
}
