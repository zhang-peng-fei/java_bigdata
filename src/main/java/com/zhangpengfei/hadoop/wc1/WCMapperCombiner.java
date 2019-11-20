package com.zhangpengfei.hadoop.wc1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * combiner是实现在Mapper端进行key的归并，combiner具有类似本地的reducer功能。
 * 如果不用combiner，那么，所有的结果都是reducer完成，效率会相对低下。使用combiner，先完成在Mapper的本地聚合，从而提升速度。
 *
 * @author 张朋飞
 */
public class WCMapperCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count=0;
        for (LongWritable value : values) {

            count += value.get();

        }

        context.write(key,new LongWritable(count));
    }
}
