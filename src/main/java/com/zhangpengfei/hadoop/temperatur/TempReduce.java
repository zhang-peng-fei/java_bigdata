package com.zhangpengfei.hadoop.temperatur;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempReduce  extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int max = 0;
        int min = Integer.MAX_VALUE;
        for (IntWritable value : values) {

            int v = value.get();
            if (max < v) {
                max = v;
            }
            if (min < v) {
                min = v;
            }

        }

        context.write(key, new Text(min + ""+max));
    }
}
