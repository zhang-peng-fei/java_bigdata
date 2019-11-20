package com.zhangpengfei.storm.trident.test1;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class WcPrintFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        tuple.getIntegerByField("line");

        return false;
    }
}
