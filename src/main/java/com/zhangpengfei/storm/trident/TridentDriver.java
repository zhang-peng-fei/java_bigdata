package com.zhangpengfei.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @author 张朋飞
 */
public class TridentDriver {

    public static void main(String[] args) throws Exception {
        Fields fields = new Fields("name", "age");
        FixedBatchSpout spout =
                new FixedBatchSpout(fields, 100, new Values("zhangsan", 23),
                        new Values("lisi", 24), new Values("wangwu", 25));
        //
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);
        stream.each(fields, new InfoPrintFilter())
                .each(new Fields("name"), new GenderFunction(), new Fields("gender"))
                .each(new Fields("name"), new NameFilter())
                // CombinerAggregator实现
//                .partitionAggregate(new AgeCombinerAggregator(), new Fields("result"));
                // ReducerAggregator实现
                .partitionAggregate(new AgeReducerAggregator(), new Fields("result"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("topology1", new Config(), topology.build());


    }
}
