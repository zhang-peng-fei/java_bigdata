package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReducer2 extends Reducer<Text, Flow, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {

        Iterator<Flow> iter = values.iterator();
        String phone = key.toString();
        String name = "";
        long flow = 0;
        while(iter.hasNext()){
            Flow fb = iter.next();
            name = fb.getName();
            flow += fb.getFlow();
        }
        context.write(new Text(phone+" "+name+" "+flow), NullWritable.get());

    }
}
