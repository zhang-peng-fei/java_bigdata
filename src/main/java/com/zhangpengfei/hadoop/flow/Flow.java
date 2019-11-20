package com.zhangpengfei.hadoop.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 写一个Bean实现Writable接口，实现其中的write和readFields方法，注意这两个方法中属性处理的顺序和类型
 * 此后这个类的对象就可以用于MR了
 *
 */
public class Flow implements Writable {

    private String phone;
    private String addr;
    private String name;
    private long flow;

    public Flow(String phone, String addr, String name, long flow) {
        this.phone = phone;
        this.addr = addr;
        this.name = name;
        this.flow = flow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeUTF(addr);
        out.writeUTF(name);
        out.writeLong(flow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.addr = in.readUTF();
        this.name = in.readUTF();
        this.flow = in.readLong();

    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getFlow() {
        return flow;
    }

    public void setFlow(long flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return "Flow{" +
                "phone='" + phone + '\'' +
                ", addr='" + addr + '\'' +
                ", name='" + name + '\'' +
                ", flow=" + flow +
                '}';
    }
}
