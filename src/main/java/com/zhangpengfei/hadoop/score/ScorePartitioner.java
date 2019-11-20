package com.zhangpengfei.hadoop.score;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 *
 * @author 张朋飞
 */
public class ScorePartitioner extends Partitioner<Text, ScoreBean> {

    @Override
    public int getPartition(Text text, ScoreBean scoreBean, int i) {
        return scoreBean.getMonth() - 1;
    }
}
