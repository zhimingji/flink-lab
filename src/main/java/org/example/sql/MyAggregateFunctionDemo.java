package org.example.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义函数（UDF）-聚合函数（Aggregate Functions）
 */
public class MyAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 姓名，分数，权重
        DataStreamSource<Tuple3<String, Integer, Integer>> scoreWeightDS = env.fromElements(
                Tuple3.of("Bob", 80, 20),
                Tuple3.of("Bob", 90, 40),
                Tuple3.of("Bob", 100, 40),
                Tuple3.of("Lili", 100, 40),
                Tuple3.of("Lili", 70, 20),
                Tuple3.of("Lili", 60, 40)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1. 流转表
        Table sensorTable = tableEnv.fromDataStream(scoreWeightDS, $("name"),$("score"),$("weight"));
        tableEnv.createTemporaryView("score", sensorTable);

        //TODO 2. 注册函数
        tableEnv.createTemporaryFunction("MyAggregateFunction",MyAggregateFunction.class);

        // TODO 3. 调用自定义函数
        // sql用法
        tableEnv.sqlQuery("select name, MyAggregateFunction(score, weight) as scoreWeight from score group by name")
                .execute()
                .print();

    }

    // TODO 继承 AggregateFunction< 返回类型，累加器类型<加权总和，权重总和> >
    /**
     * 定义聚合函数 MyAggregateFunction
     */
    //泛型： 第一个是返回类型， 第二个是累加器类型<加权总和， 权重总和>
    public static class MyAggregateFunction extends AggregateFunction<Double, Tuple2<Integer, Integer>>{

        @Override
        public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }

        /**
         * 累加计算的方法， 每来一行数据都会调用一次
         * @param acc 累加器类型
         * @param score 第一个参数： 分数
         * @param weight 第二个参数： 权重
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer score, Integer weight) {
            acc.f0 += score * weight; // 加权总和 = 分数1 * 权重1 + 分数2 * 权重2 + ...
            acc.f1 += weight; // 权重和 = 权重1 + 权重2 + ...
        }
    }
}
