package com.zhangpengfei.hadoop.wc1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("map in："+ key.get() + "  " + value.toString());

        String line = value.toString();
        String[] lines = line.split(" ");
        for (String s : lines) {
            System.out.println(s);

        }
    }
}
