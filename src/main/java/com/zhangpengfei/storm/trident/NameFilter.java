package com.zhangpengfei.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author 张朋飞
 */
public class NameFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {


            if ("zhangsan".equals(tuple.getStringByField("name"))) {
                return true;
            }else{
                return false;
            }

    }
}
