package com.zhangpengfei.storm.concurrent;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 *
 * ①现在，我们先来设定worker进程数量=2（默认是1）
 * config.setNumWorkers(2);
 *
 * ②接下来，我们更改executor和task的并发度
 *
 * builder.setSpout(spout_id,spout,2) //将spout的executor并发度设为2。此外，如果不设定task并发度，则task的并发度也为2，因为默认是一个线程执行一个task。
 * builder.setBolt(split_bolt_id,splitBolt,2).setNumTasks(4).shuffleGrouping(spout_id);//将splitBolt的线程并发度设为2，task并发度为4。在这种情况下，相当于一个executor执行两个splitBolt的task实例
 * builder.setBolt(wordcount_bolt_id,wordcountBolt,4)……//设定wordcountBolt的并发度，
 * 此外，ReportBolt的并发度未做设置，所以默认都是一个线程处理一个对应的task实例
 * @author 张朋飞
 */
public class ConTopulogy {

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new BaseRichSpout() {
            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

            }

            @Override
            public void nextTuple() {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }
        }, 2).setNumTasks(2);
        builder.setBolt("", new BaseRichBolt() {
            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(Tuple input) {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }
        }, 3).setNumTasks(2);


    }
}
