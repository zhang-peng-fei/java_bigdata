package com.zhangpengfei.hadoop.score;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReduce extends Reducer<Text, ScoreBean, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<ScoreBean> values, Context context) throws IOException, InterruptedException {

        String result = key.toString() + "  ";
        int sum = 0;
        for (ScoreBean value : values) {

            result += value.getSubject() + " " + value.getScore();
            sum += value.getScore();
        }

        result += sum;
        context.write(new Text(result), NullWritable.get());
    }
}
