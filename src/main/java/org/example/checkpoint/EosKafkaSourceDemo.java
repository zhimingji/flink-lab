package org.example.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class EosKafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 消费 被2PC 写入的topic
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop1:9092,hadoop2:9092,hadoop3:9092")//指定kafka节点的地址和端口
                .setGroupId("flinkGroup")//指定消费者组的id
                .setTopics("topic_test")//指定消费的主题
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定反序列化器，这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())
                //TODO 作为下游的消费者，要设置事务的隔离级别 = read_committed（读已提交）
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .build();//flink消费kafka的策略


        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkaSource").print();

        env.execute();
    }
}

/**
 * kafka消费者的参数：
 *  auto.reset.offsets
 *      earliest: 如果有offset,从offset继续消费，如果没有offset,从最早消费
 *      latest:   如果有offset,从offset继续消费，如果没有offset,从最新消费
 *
 * flink的kafkaSource: offset消费策略：OOffsetsInitializer，默认是earliest
 *      earliest: 一定从最早消费
 *      latest:   一定从最新消费
 * */

/**
 * 碰到的坑：
 * hadoop1有外网ip,hadoop2和hadoop3使用的是阿里云的内网ip,
 * 导致idea本地无法直接访问kafka获取元数据
 * */

