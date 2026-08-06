package org.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// DataSet API实现（不退推荐）
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        //,.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSource<String>  linesDs = env.readTextFile("src/main/java/org/example/input/word.txt");

        //切分转换（word,1）
        FlatMapOperator<String, Tuple2<String,Integer>> wordAndOne = linesDs.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");
                for (String word: words){
                    Tuple2<String,Integer> wordTuple2 = Tuple2.of(word,1);
                    out.collect(wordTuple2);
                }
            }
        });

        //按照word分组
        UnsortedGrouping<Tuple2<String,Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        //各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sumValue = wordAndOneGroupBy.sum(1);

        sumValue.print();
    }
}
