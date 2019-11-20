package com.zhangpengfei.hadoop.pro1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * 如何能够实现指定排序规则？
 * @author 张朋飞
 */
public class ProBean implements WritableComparable<ProBean> {

    private String name;
    private long profit;


    @Override
    public int compareTo(ProBean o) {
        return (int) (this.profit - o.getProfit());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeLong(profit);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.profit = dataInput.readLong();

    }

    @Override
    public String toString() {
        return "ProBean{" +
                "name='" + name + '\'' +
                ", profit=" + profit +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getProfit() {
        return profit;
    }

    public void setProfit(long profit) {
        this.profit = profit;
    }
}
