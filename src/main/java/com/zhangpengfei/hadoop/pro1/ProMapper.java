package com.zhangpengfei.hadoop.pro1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *
 *
 */
public class ProMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("  ");
        String name = lines[1];
        long fit = Long.valueOf(lines[2]) - Long.valueOf(lines[3]);

        context.write(new Text(name), new LongWritable(fit));

    }
}
