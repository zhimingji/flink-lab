package org.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;
import org.example.split.WaterSensorMapFunction;

public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop1", 7777)
                .map(new WaterSensorMapFunction());

        //配置流
        DataStreamSource<String> configDS = env.socketTextStream("hadoop1", 8888);

        //TODO 1.将配置流广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast_state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        //TODO 2.把数据流和广播后的配置流connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        //TODO 3.调用process
        sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {

            /**
             * 数据流的处理方法：数据流只能读取广播状态，不能修改
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //TODO 5.通过上下文获取广播状态，取出里面的值（只读，不能修改）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                Integer threshold = broadcastState.get("threshold");
                //判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                threshold = (threshold == null ? 0 : threshold);
                if(value.getVc() > threshold){
                    out.collect(value + ",水位超过指定的阈值：" + threshold + "!!!!");
                }
            }

            /**
             * 广播后的配置流的处理方法: 只有广播流才能修改广播状态
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {

                //TODO 4.通过上下文获取广播状态，往里面写数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                broadcastState.put("threshold",Integer.parseInt(value));
            }
        }).print();

        env.execute();
    }


}
