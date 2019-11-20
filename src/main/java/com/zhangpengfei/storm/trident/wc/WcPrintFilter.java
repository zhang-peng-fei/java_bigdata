package com.zhangpengfei.storm.trident.wc;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class WcPrintFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println(tuple.getStringByField("key") + "=" + tuple.getIntegerByField("val"));

        return false;
    }
}
