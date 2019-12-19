package com.zhangpengfei.kafka;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class LogConsumer {

    public static void main(String[] args) {
        //设置sasl文件的路径
        KafkaUtil.configureSasl();
        //设置kafka consumer的一些参数配置
        Properties props = KafkaUtil.loadAllProperties();

        //接入协议，目前支持使用SASL_SSL协议接入
        props.put("security.protocol", "SASL_PLAINTEXT");
        //SASL鉴权方式，保持不变
        props.put("sasl.mechanism", "PLAIN");

        //设置消费groupId
        props.put("group.id", "op_log_consumer");

        KafkaConsumer consumer = new KafkaConsumer<>(props);

        List<String> subscribedTopics = new ArrayList<>();
        subscribedTopics.add("log_dls");
        consumer.subscribe(subscribedTopics);

        System.err.println(consumer.listTopics().keySet());
        //循环消费消息
       /* while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }*/
    }

}
