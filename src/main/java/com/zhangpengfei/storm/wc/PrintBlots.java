package com.zhangpengfei.storm.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrintBlots extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector  = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getStringByField("key");
        Integer val = tuple.getIntegerByField("val");

        System.out.println("key:" + key + ",val:" +val );

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
