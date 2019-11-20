package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 分区操作是shuffle操作中的一个重要过程，作用就是将map的结果按照规则分发到不同reduce中进行处理，从而按照分区得到多个输出结果
 * Partitioner是分区的基类，如果需要定制partitioner需要继承该类
 * HashPartitioner是mapreduce的默认partitioner。计算方法是
 * reducer=(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks
 * 注：默认情况下，reduceTask数量为1
 * 很多时候MR自带的分区规则并不能满足我们需求，为了实现特定的效果，可以需要自己来定义分区规则。
 *
 * @author 张朋飞
 */
public class FlowPartitioner extends Partitioner<Text,Flow> {

    private static Map<String,Integer> addrMap;
    static{
        addrMap =new HashMap();
        addrMap.put("bj", 0);
        addrMap.put("sh", 1);
        addrMap.put("sz", 2);
    }


    @Override
    public int getPartition(Text text, Flow flow, int i) {

        String addr = flow.getAddr();
        Integer partition = addrMap.get(addr);
        if (partition == null) {
            return i-1;
        }
        return partition;
    }

}
