package org.example.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义函数（UDF）-表聚合函数（Table Aggregate Functions）
 */
public class MyTableAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> numDS = env.fromElements(5, 7, 6, 9, 2, 5, 16, 4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1. 流转表
        Table numTable = tableEnv.fromDataStream(numDS, $("num"));

        //TODO 2. 注册函数
        tableEnv.createTemporaryFunction("Top2",Top2.class);

        // TODO 3. 调用自定义函数
        // table api
        numTable
                .flatAggregate(
                        call("Top2",$("num")).as("value","rank")
                ).select($("value"), $("rank"))
                .execute()
                .print();
    }


    /**
     * 定义表聚合函数 Top2
     */
    //返回的类型（数值，排名）--> (16,1) (9,2)
    //累加器类型（第一大的数，第二大的数） --> （16,9）
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>{

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        /**
         * 每来一条数据调用一次， 比较大小， 更新最大的前两个数到 acc 中
         * @param acc 累加器
         * @param num 过来的数据
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer num) {
            if(num > acc.f0) {
                // 新来的变第一， 原来的第一变第二
                acc.f1 = acc.f0;
                acc.f0 = num;
            }else if ( num > acc.f1) {
                // 新来的变第二， 原来的第二不要了
                acc.f1 = num;
            }
        }

        /**
         * 输出结果：（数值，排名）两条最大的
         * @param acc 累加器
         * @param out 采集器<返回类型>
         */
        public void emitValue(Tuple2<Integer, Integer> acc, Collector< Tuple2<Integer, Integer>> out) {
            if (acc.f0 != 0){
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != 0){
                out.collect(Tuple2.of(acc.f1, 2));
            }
        }
    }
}
