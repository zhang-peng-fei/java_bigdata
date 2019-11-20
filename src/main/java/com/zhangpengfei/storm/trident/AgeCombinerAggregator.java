package com.zhangpengfei.storm.trident;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class AgeCombinerAggregator implements CombinerAggregator<Integer> {
    @Override
    public Integer init(TridentTuple tuple) {
        Integer age = tuple.getIntegerByField("age");
        return age;
    }

    @Override
    public Integer combine(Integer val1, Integer val2) {
        int ageSum = val1 + val2;
        System.out.println(ageSum);
        return ageSum;
    }

    @Override
    public Integer zero() {
        return 0;
    }
}
