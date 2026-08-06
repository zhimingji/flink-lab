package org.example.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.bean.WaterSensor;

public class MapFunctionImpl implements MapFunction<WaterSensor, String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
