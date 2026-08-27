package org.example.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.example.bean.WaterSensor;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义函数（UDF）-标量函数（Scalar Functions）
 */
public class MyScalarFunctionDemo {
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

        //TODO 2. 注册函数
        tableEnv.createTemporaryFunction("HashFunction",HashFunction.class);

        // TODO 3. 调用自定义函数
        //3.1 sql用法
        tableEnv.sqlQuery("select HashFunction(id) from sensor").execute().print();

        //3.2 table api 用法
//        sensorTable.select(call("HashFunction",$("id"))).execute().print();

    }

    /**
     * 定义标量函数HashFunction
     */
    public static class HashFunction extends ScalarFunction {
        //接受任意类型的输入，返回INT类型输出
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object t) {
            return t.hashCode();
        }
    }
}
