package com.zhangpengfei.hadoop.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job_score");
        job.setJarByClass(ScoreDriver.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置分区类和分区的个数
        job.setPartitionerClass(ScorePartitioner.class);
        job.setNumReduceTasks(3);
        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.76:9000/park/score/"));
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.76:9000/park/score/result"));

        if (!job.waitForCompletion(true)) {
            return;
        }
    }

}

