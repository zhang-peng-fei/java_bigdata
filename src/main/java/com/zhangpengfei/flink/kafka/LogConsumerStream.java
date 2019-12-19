package com.zhangpengfei.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class LogConsumerStream {
    public static void main(String[] args){
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.78.135:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "192.168.78.135:2181");
        properties.setProperty("group.id", "op_log_consumer");
        // new consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log_dls", new SimpleStringSchema(), properties);
        consumer.setStartFromLatest();

        DataStream<String> stream = env
                .addSource(consumer);

        stream.print();


        try {
            env.execute("<<<<<<<<<<<<<<<is starting>>>>>>>>>>>>>>>>>>>>>>>");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
