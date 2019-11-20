package com.zhangpengfei.hadoop.score;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ScoreBean implements Writable {

    private int month;
    private String name;
    private String subject;
    private double score;

    @Override
    public String toString() {
        return "ScoreBean{" +
                "month=" + month +
                ", name='" + name + '\'' +
                ", subject='" + subject + '\'' +
                ", score=" + score +
                '}';
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(month);
        out.writeUTF(name);
        out.writeUTF(subject);
        out.writeDouble(score);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        month = in.readInt();
        name = in.readUTF();
        subject = in.readUTF();
        score = in.readDouble();

    }
}
