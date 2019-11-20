package com.zhangpengfei.hadoop.pro1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 月份 姓名 收入  成本   ls 2750,3366   ls 6116
 * 1 ls 2850 100
 * 2 ls 3566 200
 * 3 ls 4555 323
 * 1 zs 19000 2000
 * 2 zs 28599 3900
 * 3 zs 34567 5000
 * 1 ww 355 10
 * 2 ww 555 222
 * 3 ww 667 192
 * @author 张朋飞
 */
public class ProDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "ProJob");

        job.setJarByClass(ProDriver.class);
        job.setMapperClass(ProMapper.class);
        job.setReducerClass(ProReduce.class);
        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.68:9000/profit"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.68:9000/profit/result"));
        if (!job.waitForCompletion(true)) {
            return;
        }

    }
}
