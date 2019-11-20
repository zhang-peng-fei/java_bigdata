package com.zhangpengfei.storm.num;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class NumberBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer randomInt = tuple.getIntegerByField("number");

        if (randomInt < 10) {
            collector.emit("moreThan", new Values(randomInt));
        } else {
            collector.emit("lessThan", new Values(randomInt));
        }
//        System.out.println(randomInt );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("lessThan", new Fields("number"));
        outputFieldsDeclarer.declareStream("moreThan", new Fields("number"));
    }
}
