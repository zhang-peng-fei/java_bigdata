package com.zhangpengfei.storm.kafka.trident.demo.trident;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
/**
 * @author 张朋飞
 */
public class TridentKafkaClientTopologyWildcardTopics extends TridentKafkaClientTopologyNamedTopics {/*
    private static final Pattern TOPIC_WILDCARD_PATTERN = Pattern.compile("test-trident(-1)?");

    @Override
    protected KafkaTridentSpoutConfig<String,String> newKafkaSpoutConfig(String bootstrapServers) {
        return KafkaTridentSpoutConfig.builder(bootstrapServers, TOPIC_WILDCARD_PATTERN)
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
                .setRecordTranslator((r) -> new Values(r.value()), new Fields("str"))
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientTopologyWildcardTopics().run(args);
    }*/
}
