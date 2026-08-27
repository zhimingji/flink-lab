package org.example.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义函数（UDF）-函数（Table Functions）
 */
public class MyTableFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> strDS = env.fromElements(
                "hello flink",
                "good evening girls",
                "do you like spiderMan"
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1. 流转表
        Table sensorTable = tableEnv.fromDataStream(strDS, $("words"));
        tableEnv.createTemporaryView("str", sensorTable);

        //TODO 2. 注册函数
        tableEnv.createTemporaryFunction("SplitFunction",SplitFunction.class);

        // TODO 3. 调用自定义函数
        // sql用法
        tableEnv
                // 3.1 交叉联结
//                .sqlQuery("select words,word,length from str, lateral table(SplitFunction(words))")
                // 3.2 带 on  true 条件的 左联结
//                .sqlQuery("select words,word,length from str left join lateral table(SplitFunction(words)) on true")
                // 3.3 重命名侧向表中的字段
                .sqlQuery("select words,newWord,newLength from str left join lateral table(SplitFunction(words)) as T(newWord,newLength) on true")
                .execute()
                .print();

    }

    /**
     * 定义表函数 SplitFunction
     */
    //类型标注： Row 包含两个字段：word 和 length, 可以被用于 select
    @FunctionHint(output = @DataTypeHint("Row<word String,length INT>"))
    public static class SplitFunction extends TableFunction<Row>{
        /**
         * 返回是void, 用collect方法输出
         * @param str
         */
        public void eval(String str) {
            for (String word : str.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}
