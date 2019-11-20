package com.zhangpengfei.storm.trident.wc;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @author 张朋飞
 */
public class WcDriver {
    public static void main(String[] args) throws Exception {

        Config config = new Config();
        Fields fields = new Fields("word");
        int maxBatchSize = 100;

        FixedBatchSpout spout = new FixedBatchSpout(fields, maxBatchSize,
                new Values("hello world"),
                new Values("hello hadoop"),
                new Values("hello storm"),
                new Values("hello world"),
                new Values("hello hadoop"),
                new Values("hello world"));

        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("wc", spout);
        stream.partitionAggregate(new Fields("wc"), new WcBaseAggregator(), new Fields("key", "val"))
                .each(new Fields("key", "val"), new WcPrintFilter());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("to", config, topology.build() );
        Utils.sleep(5000);
        cluster.killTopology("to");
        cluster.shutdown();

    }
}
