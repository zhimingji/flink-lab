package org.example.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private long delayTs;//乱序等待时间
    private long maxTs;//用来保存当前为止最大的事件时间

    public MyPeriodWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = delayTs;
    }

    /**
     * 每条数据来，都会调用一次：用来提取最大的事件时间，保存下来
     * @param event
     * @param eventTimestamp 提取到的数据的 事件时间
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs,eventTimestamp);
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs);
    }

    /**
     * 周期性调用： 发射watermark
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs - 1));
    }
}
