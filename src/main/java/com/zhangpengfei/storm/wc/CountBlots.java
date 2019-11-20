package com.zhangpengfei.storm.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBlots extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Integer> map = new HashMap();

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String wc = tuple.getStringByField("wc");
        if (map.containsKey(wc)) {
            map.put(wc, map.get(wc) + 1);
        } else {
            map.put(wc, 1);
        }
        collector.emit(new Values(wc, map.get(wc)));
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key","val"));
    }
}
