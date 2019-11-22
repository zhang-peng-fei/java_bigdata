package com.zhangpengfei.util;

import java.util.Properties;

/**
 * @author 张朋飞
 */
public class KafkaUtil {


    public  static Properties getKafkaProp() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.78.135:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public static String getDefaultTopic() {
        return "topic1";
    }
}
