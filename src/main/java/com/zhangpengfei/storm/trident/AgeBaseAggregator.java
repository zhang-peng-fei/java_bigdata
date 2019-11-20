package com.zhangpengfei.storm.trident;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class AgeBaseAggregator extends BaseAggregator<Integer> {
    @Override
    public Integer init(Object batchId, TridentCollector collector) {
        return 0;
    }

    @Override
    public void aggregate(Integer val, TridentTuple tuple, TridentCollector collector) {

        Integer age = tuple.getIntegerByField("age");
        int i = val + age;
        collector.emit(new Values(i));
    }

    @Override
    public void complete(Integer val, TridentCollector collector) {

    }
}
