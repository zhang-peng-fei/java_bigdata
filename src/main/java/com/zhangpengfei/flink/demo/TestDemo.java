package com.zhangpengfei.flink.demo;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

public class TestDemo {
    public static void main(String[] args){
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
//            env.execute("is starting>>>>>>>>>>>>>>>>>>");
            localEnvironment.execute("is starting");
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
