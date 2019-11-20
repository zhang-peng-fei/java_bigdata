package com.zhangpengfei.storm.num;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author 张朋飞
 */
public class NumberTopology {

    public static void main(String[] args) throws Exception {

        Config config=new Config();

        NumberSpout numberSpout=new NumberSpout();
        NumberBolt numberBolt=new NumberBolt();
        MoreThanBolt moreThanBolt=new MoreThanBolt();
        LessThanBolt lessThanBolt=new LessThanBolt();

        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout("Number_Spout", numberSpout);
        builder.setBolt("Number_Bolt", numberBolt).shuffleGrouping("Number_Spout");
        builder.setBolt("MoreThan_Bolt",moreThanBolt).shuffleGrouping("Number_Bolt","moreThan");
        builder.setBolt("LessThan_Bolt",lessThanBolt).shuffleGrouping("Number_Bolt","lessThan");

        StormTopology topology=builder.createTopology();
        LocalCluster cluster=new LocalCluster();

        cluster.submitTopology("Number_Topology",config,topology);



    }
}
