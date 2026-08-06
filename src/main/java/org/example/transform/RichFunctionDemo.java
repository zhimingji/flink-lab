package org.example.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);



//        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
//        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//
//                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
//                        "子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() +
//                        "调用open()");
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
//                        "子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() +
//                        "调用close()");
//            }
//
//
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value + 1;
//            }
//        });

        DataStreamSource<String> source = env.socketTextStream("hadoop1", 7777);


        /**
         * todo RichxxxxFunction:富函数
         * 1、多了生命周期管理方法：
         *      open():每个子任务，在启动时，调用一次
         *      close():每个子任务，在结束时，调用一次
         *          => 如果是flink程序异常挂掉，不会调用close
         *          => 如果是正常调用 cancel命令，可以close
         *
         * 2、多了一个运行时上下文
         *      可以获取一些运行时的环境信息，比如：子任务编号、名称、其他的....
         *
         *
         * */
        SingleOutputStreamOperator<String> map = source.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        "子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() +
                        "调用open()");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号=" + getRuntimeContext().getIndexOfThisSubtask() +
                        "子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks() +
                        "调用close()");
            }

            @Override
            public String map(String value) throws Exception {
                return value+"!";
            }
        });

        map.print();
        env.execute();
    }
}
