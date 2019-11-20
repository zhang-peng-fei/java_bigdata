package com.zhangpengfei.storm.wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WcSpout extends BaseRichSpout {

    private String[] data = new String[]{
            "hello storm",
            "hello world",
            "hello hadoop",
            "hello world"
    };

    private SpoutOutputCollector collector;
    private int i = 0;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (i == data.length) {

        } else {
            collector.emit(new Values(data[i]));
            i++;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("str"));
    }
}
