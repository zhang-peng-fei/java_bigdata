package com.zhangpengfei.storm.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WcTopolegy {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        // 本地集群模式
//        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcSpout", new WcSpout());
        builder.setBolt("splitBlot", new SplitBlots()).shuffleGrouping("wcSpout");
        builder.setBolt("wcBlot", new CountBlots()).fieldsGrouping("splitBlot", new Fields("key"));
        builder.setBolt("printBlot", new PrintBlots()).globalGrouping("wcBlot");

        StormTopology topology = builder.createTopology();
        // 集群模式
        StormSubmitter.submitTopology("wcTopology", config, topology);
//        cluster.submitTopology("wcTopology", config, topology);

        Utils.sleep(10000);
//        cluster.shutdown();

    }
}
