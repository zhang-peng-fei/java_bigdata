package com.zhangpengfei.hadoop.num;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumReduce  extends Reducer<IntWritable, LongWritable, LongWritable, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long min = Long.MAX_VALUE;
        for (LongWritable value : values) {

            if (min < value.get())
            {
                min = value.get();
            }
        }

        context.write(new LongWritable(min), NullWritable.get());
    }
}
