package org.example.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> socketDS = env.socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());


        /**
         * TODO 写入mysql
         * 1、只能用老的sink写法： addSink
         * 2、JdbcSink的4个参数：
         *      第一个参数：执行的sql,一般是insert into
         *      第二个参数：预编译sql,对占位符填充
         *      第三个参数：执行选项  ---》攒批、重试
         *      第四个参数：连接选项  ---》url、用户名、密码
         */
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("insert into ws values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    //收到一条WaterSensor，如何去填充占位符
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()

        );

        socketDS.addSink(jdbcSink);

        env.execute();
    }
}
