package com.zhangpengfei.hadoop.wc1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WCReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        //获取输入的value，并获取它的迭代器
        Iterator<LongWritable> vals = values.iterator();
        //定义变量：当前当前的总数量
        long num = 0;
        String inval = "";
        //遍历迭代器进行累加
        while (vals.hasNext()) {
            long tmp = vals.next().get();
            num += tmp;
            inval += tmp + ",";
        }
        System.out.println("reducer_in:" + key.toString() + " " + inval);
        System.out.println("reducer_out:" + key.toString() + " " + num);
        //输出结果
        context.write(key, new LongWritable(num));

    }
}
