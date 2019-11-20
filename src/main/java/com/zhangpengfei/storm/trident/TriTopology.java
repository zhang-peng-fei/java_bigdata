package com.zhangpengfei.storm.trident;

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
public class TriTopology {
    public static void main(String[] args) throws Exception {
        Config config = new Config();

        //声明spout发生元组的key值
        Fields fields = new Fields("age", "name");

        //定义一个批次中最多处理的信息数量
        int maxBatchSize = 100;


        @SuppressWarnings("unchecked")
        FixedBatchSpout numberSpout = new FixedBatchSpout(fields, maxBatchSize,
                new Values(23, "tom"), new Values(30, "li"), new Values(40, "wang"), new Values(60, "zhao"));

        //设置消息源是否循环发送消息，true是循环发送，false是不循环
        numberSpout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        //通过spout获取流
        Stream stream = topology.newStream("s1", numberSpout);

        // TODO

        //本例是以本地集群方式启动
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident_number_topology", config, topology.build());

        //运行5s后 退出
        Utils.sleep(5000);
        cluster.killTopology("trident_number_topology");
        cluster.shutdown();

    }
}
