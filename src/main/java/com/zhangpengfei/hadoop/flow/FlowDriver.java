package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobName");
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        // todo 设置分区类
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.97:9000/park/flow/flow.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.97:9000/park/flow/result"));


        if (!job.waitForCompletion(true)) {
            return;
        }

    }
}
