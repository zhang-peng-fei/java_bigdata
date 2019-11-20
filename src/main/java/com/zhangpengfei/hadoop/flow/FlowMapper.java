package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        //13877779999 bj zs 2145
        String data[] = line.split(" ");
        String phone = data[0];
        String addr = data[1];
        String name = data[2];
        String flow = data[3];
        context.write(new Text(data[0]), new Text(phone+","+addr+","+name+","+flow));

    }
}
