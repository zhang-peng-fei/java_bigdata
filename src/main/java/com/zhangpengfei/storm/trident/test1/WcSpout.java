package com.zhangpengfei.storm.trident.test1;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author 张朋飞
 */
public class WcSpout extends BaseRichSpout {

    private Values[] values = new Values[]{
            new Values("hello world"),
            new Values("hello hadoop"),
            new Values("hello storm"),
            new Values("hello world"),
            new Values("hello hadoop"),
            new Values("hello world")
    };

    private int i = 0;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (i == values.length) {

        } else {
            collector.emit(values[i]);
            i++;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
