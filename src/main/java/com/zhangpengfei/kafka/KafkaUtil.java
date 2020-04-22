package com.zhangpengfei.kafka;


import java.util.Properties;

public class KafkaUtil {
    /**
     * 加载kafka.properties文件中的所有kafka配置信息
     */
    public static Properties loadAllProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.142.117.55:9093,10.142.117.56:9093,10.142.117.57:9093");
        props.put("enable.auto.commit", false);
        props.put("session.timeout.ms", 30000);
        //每次poll最多获取100条数据
        props.put("max.poll.records", 1);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    /**
     * 加载kafka认证配置文件
     */
    public static void configureSasl() {
        //如果用-D或者其它方式设置过，这里不再设置
        //这个路径必须是一个文件系统可读的路径，不能被打包到jar中
        String s = System.setProperty("java.security.auth.login.config", "/data1/tydic/web/jaas.conf");
        System.out.println(s);
//        System.setProperty("java.security.auth.login.config", "f:/tydic/jaas.conf");
    }
}
