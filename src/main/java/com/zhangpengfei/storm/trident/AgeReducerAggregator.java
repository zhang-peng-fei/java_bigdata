package com.zhangpengfei.storm.trident;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class AgeReducerAggregator implements ReducerAggregator<Integer> {
    @Override
    public Integer init() {
        return 0;
    }

    @Override
    public Integer reduce(Integer curr, TridentTuple tuple) {
        int age = curr + tuple.getIntegerByField("age");
        return age;
    }
}
