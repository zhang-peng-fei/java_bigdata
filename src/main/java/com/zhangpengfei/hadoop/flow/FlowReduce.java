package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReduce extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //13877779999 bj zs 2145
        String phone = "";
        String name = "";
        long flow = 0;
        Iterator<Text> iter = values.iterator();
        if (iter.hasNext()) {
            String data[] = iter.next().toString().split(",");
            phone = data[0];
            name = data[2];
            flow += Long.parseLong(data[3]);
        }
        while (iter.hasNext()) {
            String data[] = iter.next().toString().split(",");
            flow += Long.parseLong(data[3]);
        }
        context.write(new Text(phone + " " + name + " " + flow), NullWritable.get());

    }
}
