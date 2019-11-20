package com.zhangpengfei.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseUtil {

    public  static Configuration getHbaseConfig() {
        Configuration config = HBaseConfiguration.create();
        // 显式配置 hbase zookeeper 地址
        config.set("hbase.zookeeper.quorum", "192.168.78.135");
        config.set("hbase.table.sanity.checks", "false");
        // hdfs 配置和 hbase 配置 (hbase-site.xml, core-site.xml)
        config.addResource(new Path(CommUtils.getBasicPath() + "/config/hbase-site.xml"));
        config.addResource(new Path(CommUtils.getBasicPath() + "/config/core-site.xml"));
        return config;
    }
}
