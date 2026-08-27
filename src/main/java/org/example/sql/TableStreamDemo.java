package org.example.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.bean.WaterSensor;

/**
 * 表（Table）和流（DataStream）的转换
 */
public class TableStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 2),
                new WaterSensor("s1", 1L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1. 流转表
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor", sensorTable);

        Table filteTable = tableEnv.sqlQuery("SELECT id, ts, vc FROM sensor where vc > 1;");
        Table sumTable = tableEnv.sqlQuery("select id,sum(vc) from sensor group by id;");

        //TODO 2. 表转流
        //2.1 追加流
        tableEnv.toDataStream(filteTable, WaterSensor.class).print("filter");
        //2.2 changelog流
        tableEnv.toChangelogStream(sumTable).print("sum");

        env.execute();
    }
}
