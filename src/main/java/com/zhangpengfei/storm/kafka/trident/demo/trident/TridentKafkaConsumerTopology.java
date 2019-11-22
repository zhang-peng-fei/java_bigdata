package com.zhangpengfei.storm.kafka.trident.demo.trident;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author 张朋飞
 */
public class TridentKafkaConsumerTopology {
    protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaConsumerTopology.class);

    /**
     * Creates a new topology that prints inputs to stdout.
     * @param tridentSpout The spout to use
     */
    public static StormTopology newTopology(ITridentDataSource tridentSpout) {
        final TridentTopology tridentTopology = new TridentTopology();
        final Stream spoutStream = tridentTopology.newStream("spout", tridentSpout).parallelismHint(2);
        spoutStream.each(spoutStream.getOutputFields(), new Debug(false));
        return tridentTopology.build();
    }
}
