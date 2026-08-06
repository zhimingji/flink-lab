package org.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

import java.time.Duration;

public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("hadoop1",7777)
                .map(new MyMapFunction())
                .print();

        env.execute();
    }

    //TODO 1.实现 CheckpointedFunction 接口
    public static class MyMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            return ++count;
        }

        /**
         * TODO 2.本地变量持久化：将本地变量拷贝到算子状态中,开启checkpoint时才会调用
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState被调用！");
            //2.1 清空算子状态
            state.clear();
            //2.2 将本地变量添加到算子状态中
            state.add(count);
        }


        /**
         * TODO 3.初始化本地变量： 从状态中，把数据添加到本地变量，每个子任务调用一次
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState被调用！");
            //3.1 从上下文初始化算子状态
            state = context.getOperatorStateStore()
//                    .getListState(new ListStateDescriptor<Long>("state", Types.LONG));
                    .getUnionListState(new ListStateDescriptor<Long>("union-state",Types.LONG));

            //3.2 从算子状态中把数据拷贝到本地变量
            if(context.isRestored()){
                for(Long value : state.get()){
                    count += value;
                }
            }
        }
    }
}
/**
 *
 *算子状态中，list与unionlist的区别：并行度改变时，怎么重新分配状态
 * 1.list状态：轮询均分给新的并行子任务
 * 2.unionlist状态：原先的多个子任务的状态，合并一份完整的。会把完整的列表广播给新的并行子任务（每人一份完整的）
 */
