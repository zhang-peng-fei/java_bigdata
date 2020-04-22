package com.zhangpengfei.hadoop.wc2shuffle;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * @author 张朋飞
 */
public class WCGroupingComparator extends WritableComparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

        Text key1 = new Text();
        DataInputStream di = new DataInputStream(new ByteArrayInputStream(b1, s1, l1));
        try {
            key1.readFields(di);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Text key2 = new Text();
        DataInputStream di2 = new DataInputStream(new ByteArrayInputStream(b2, s2, l2));
        try {
            key2.readFields(di2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //只用ka和kb都是a-n   或都是o-z开头被分成一组  return 0
        //其他情况都不被分成一组，return -1
        if(key1.toString().matches("^[a-n][a-z]*$")&&
                key2.toString().matches("^[a-n][a-z]*$")){
            return 0;
        }else if(key1.toString().matches("^[o-z][a-z]*$")&&
                key2.toString().matches("^[o-z][a-z]*$")){
            return 0;
        }else{
            return -1;
        }

    }
}
