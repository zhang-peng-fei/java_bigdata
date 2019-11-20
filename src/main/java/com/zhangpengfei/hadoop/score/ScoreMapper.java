package com.zhangpengfei.hadoop.score;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 *
 * @author 张朋飞
 */
public class ScoreMapper extends Mapper<LongWritable, Text, Text, ScoreBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lineData = value.toString().split(" ");
        FileSplit fs = (FileSplit) context.getInputSplit();
        String fullName = fs.getPath().getName();

        ScoreBean scoreBean = new ScoreBean();
        scoreBean.setSubject(fullName.substring(0, fullName.lastIndexOf(".")));
        scoreBean.setMonth(Integer.parseInt(lineData[0]));
        scoreBean.setName(lineData[1]);
        scoreBean.setScore(Double.parseDouble(lineData[2]));
        //输出
        context.write(new Text(scoreBean.getName()), scoreBean);

    }
}
