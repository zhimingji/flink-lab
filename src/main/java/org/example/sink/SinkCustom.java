package org.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;


/**
 * 自定义写出，一般不推荐使用，尽可能使用flink现有的连接器
 */
public class SinkCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop1", 7777);

        socketDS.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<String> {
        Connection connection = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //在这里创建连接
        }

        @Override
        public void close() throws Exception {
            super.close();
            //做一些清理、销毁连接
        }

        /**
         * sink的核心哟几，写出的逻辑就写在这个方法里
         * @param value The input record.
         * @param context Additional context about the input record.
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            super.invoke(value, context);
            //写出逻辑
            //这个方法是来一条数据，调用一次，所以不要在这里创建连接对象
        }
    }

}
