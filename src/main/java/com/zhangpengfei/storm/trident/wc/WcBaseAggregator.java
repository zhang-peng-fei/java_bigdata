package com.zhangpengfei.storm.trident.wc;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 张朋飞
 */
public class WcBaseAggregator extends BaseAggregator<String> {

    private static Map<String, Integer> map = new HashMap();


    @Override

    public String init(Object batchId, TridentCollector collector) {
        return "";
    }

    @Override
    public void aggregate(String val, TridentTuple tuple, TridentCollector collector) {

        String word = tuple.getStringByField("word");
        String[] s = word.split(" ");
        for (String s1 : s) {
            if(map.containsKey(s1)){
                map.put(s1, map.get(s1)+1);
            }else{
                map.put(s1,1);
            }

        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            collector.emit(new Values(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void complete(String val, TridentCollector collector) {

    }
}
