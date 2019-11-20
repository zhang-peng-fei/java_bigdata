package com.zhangpengfei.hadoop.temperatur;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 假设我们需要处理一批有关天气的数据，其格式如下：
 * 每行一条记录。每行共24个字符（包含符号在内）
 * 第9、10、11、12字符为年份，第19、20、21、22字符代表温度，求每年的最高温度
 * 2329999919500515070000
 * 9909999919500515120022
 * 9909999919500515180011
 * 9509999919490324120111
 * 6509999919490324180078
 * 9909999919370515070001
 * 9909999919370515120002
 * 9909999919450515180001
 * 6509999919450324120002
 * 8509999919450324180078
 * 在map过程中，通过对每一行字符串的解析，得到年-温度的key-value对作为输出：
 * (1950, 0)
 * (1950, 22)
 * (1950, 11)
 * (1949, 111)
 * (1949, 78)
 * (1937, 1)
 * (1937, 2)
 * (1945, 1)
 * (1945, 2)
 * (1945, 78)
 * 在reduce过程，将map过程中的输出，按照相同的key将value放到同一个列表中作为reduce的输入
 * (1950, [0, 22, 11])
 * (1949, [111, 78])
 * (1937, [1, 2])
 * (1945, [1, 2, 78])
 * 在reduce过程中，在列表中选择出最大的温度，将年-最大温度的key-value作为输出：
 * (1950, 22)
 * (1949, 111)
 * (1937, 1)
 *
 */
public class TempeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(8, 12);

        int temperature = Integer.parseInt(line.substring(18, 22));

        context.write(new Text(year),new IntWritable(temperature));

    }
}
