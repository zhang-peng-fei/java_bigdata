package com.zhangpengfei.flink.checkpoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张朋飞
 */
public class CheckRestartStrategy {
    public static void main(String[] args) throws Exception {
        // 不重启策略
        method1();
        // 固定时间间隔重启
        method2();
        // 失败率重启
        method3();
    }

    private static void method3() throws Exception {
        /*初始化Flink流处理环境*/
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*每隔10S做一次checkpoint*/
        senv.enableCheckpointing(10000);
        /* 设置失败率重启策略，下面设置2分钟内 最大失败次数3次 重试时间间隔10秒*/
        senv.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, //
                Time.minutes(2), //time interval for measuring failure rate
                Time.seconds(10) // delay
        ));
        /*从socket接收数据*/
        DataStream<String> source = senv.socketTextStream("localhost", 9000);
        DataStream<String> filterData=source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(null==value||"".equals(value)){
                    return false;
                }
                return true;
            }
        });
        DataStream<Tuple2<String, Integer>> wordOne = filterData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                /*收到error开头的数据 手动抛出异常  进行测试*/
                if(value.startsWith("error")){
                    throw new Exception("收到error，报错！！！");
                }
                String[] lines = value.split(",");
                for(int i=0;i<lines.length;i++){
                    Tuple2<String,Integer> wordTmp=new Tuple2<>(lines[i],1);
                    out.collect(wordTmp);
                }
            }
        });
        DataStream<Tuple2<String, Integer>> wordCount = wordOne.keyBy(0).sum(1);
        wordCount.print();
        senv.execute();
    }

    private static void method2() throws Exception {
        /*初始化Flink流处理环境*/
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*每隔10S做一次checkpoint*/
        senv.enableCheckpointing(10000);
        /*采用固定时间间隔重启策略，设置重启次数和重启时间间隔*/
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,// 尝试重启的次数
                Time.seconds(2)  //重启时间间隔
        ));
        /*从socket接收数据*/
        DataStream<String> source = senv.socketTextStream("localhost", 9000);
        DataStream<String> filterData=source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(null==value||"".equals(value)){
                    return false;
                }
                return true;
            }
        });
        DataStream<Tuple2<String, Integer>> wordOne = filterData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                /*收到error开头的数据 手动抛出异常  进行测试*/
                if(value.startsWith("error")){
                    throw new Exception("收到error，报错！！！");
                }
                String[] lines = value.split(",");
                for(int i=0;i<lines.length;i++){
                    Tuple2<String,Integer> wordTmp=new Tuple2<>(lines[i],1);
                    out.collect(wordTmp);
                }
            }
        });
        DataStream<Tuple2<String, Integer>> wordCount = wordOne.keyBy(0).sum(1);
        wordCount.print();
        senv.execute();
    }

    private static void method1() throws Exception {
        /*初始化Flink流处理环境*/
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*从socket接收数据*/
        DataStream<String> source = senv.socketTextStream("localhost", 9000);
        DataStream<String> filterData=source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(null==value||"".equals(value)){
                    return false;
                }
                return true;
            }
        });
        DataStream<Tuple2<String, Integer>> wordOne = filterData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                /*收到error开头的数据 手动抛出异常  进行测试*/
                if(value.startsWith("error")){
                    throw new Exception("收到error，报错！！！");
                }
                String[] lines = value.split(",");
                for(int i=0;i<lines.length;i++){
                    Tuple2<String,Integer> wordTmp=new Tuple2<>(lines[i],1);
                    out.collect(wordTmp);
                }
            }
        });
        DataStream<Tuple2<String, Integer>> wordCount = wordOne.keyBy(0).sum(1);
        wordCount.print();
        senv.execute();
    }
}
