package org.example.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.example.bean.WaterSensor;

public class FilterFunctionImpl implements FilterFunction<WaterSensor> {
    private String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return value.getId().equals(id);
    }
}
