package com.zhangpengfei.hadoop.pro1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long fot = 0;
        for (LongWritable value : values) {

            fot += value.get();
        }

        context.write(key, new LongWritable(fot));
    }
}
