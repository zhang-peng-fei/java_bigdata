package com.zhangpengfei.hadoop.num;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 求文件中数据的最大值/最小值
 * 文件num.txt:     Mapper(LW pos,Text value,
 * 123
 * 235345
 * 234
 * 654768
 * @author 张朋飞
 */
public class NumMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Long num = Long.valueOf(value.toString());

        context.write(new IntWritable(1), new LongWritable(num));
    }
}
