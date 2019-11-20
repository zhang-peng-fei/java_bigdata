package com.zhangpengfei.hadoop.wc1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();

        Job job = Job.getInstance(config, "wordcountjob");
        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.76:9000/park/word/words.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.80.76:9000/park/word/result"));

        job.waitForCompletion(false);

    }
}
