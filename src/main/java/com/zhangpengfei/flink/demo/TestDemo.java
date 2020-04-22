package com.zhangpengfei.flink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author 张朋飞
 */
public class TestDemo {
    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Integer> data = new ArrayList<>(2);
        data.add(1);
        data.add(2);
        env.fromCollection(data)
        .print();

        try {
            env.execute("TestDemo");
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
