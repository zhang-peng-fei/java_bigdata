package com.zhangpengfei.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author 张朋飞
 * /opt/flink-1.9.0/bin/flink run -c com.zhangpengfei.flink.kafka.LogConsumerStream /data1/tydic/zpf/java_bigData-0.1.jar
 */
public class LogConsumerStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.78.135:9092");
//        properties.setProperty("zookeeper.connect", "192.168.78.135:2181");
//        String hdfsPath = "hdfs://192.168.78.135:8082/user/hive/bendi";

        String hdfsPath = "hdfs://10.142.149.245:8082/user/hive/warehouse/";
        properties.setProperty("bootstrap.servers", "10.142.117.55:9093,10.142.117.56:9093,10.142.117.57:9093");
        properties.setProperty("zookeeper.connect", "10.142.114.211:2181,10.142.114.231:2181,10.142.114.241:2181");
        properties.setProperty("group.id", "op_log_consumer");

        // new consumer
        FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<>("log_dls", new JSONKeyValueDeserializationSchema(true), properties);
        Map<KafkaTopicPartition, Long> map = new HashMap<>(16);
        map.put(new KafkaTopicPartition("log_dls", 0), 0L);
        consumer.setStartFromSpecificOffsets(map);

        BucketingSink hdfsSink = new BucketingSink(hdfsPath);
        hdfsSink.setBucketer(new MothBucketer<ApiCallLog>());
        //    hdfsSink.setWriter(new SequenceFileWriter[String])
        hdfsSink.setBatchSize(1024 * 1024 * 400);
        hdfsSink.setBatchRolloverInterval(20 * 60 * 1000);

        env.addSource(consumer)
                .addSink(hdfsSink);
        try {
            env.execute("LogConsumerStream");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
