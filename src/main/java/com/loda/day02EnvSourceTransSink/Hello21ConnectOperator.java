package com.loda.day02EnvSourceTransSink;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author loda
 * @Date 2023/4/16 23:44
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello21ConnectOperator {
    public static void main(String[] args) throws Exception {
        //获取程序运行的环境
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Integer> source1 = environment.fromElements(33, 35, 38, 45);
        DataStreamSource<String> source2 = environment.fromElements("no", "no", "yes", "no");

        ConnectedStreams<Integer, String> connectedStreams = source1.connect(source2);

        connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                if (integer > 40) {
                    return "温度[" + integer + "]异常，准备报警";
                }
                return "温度[" + integer + "]正常";
            }

            @Override
            public String map2(String s) throws Exception {
                if ("yes".equals(s)) {
                    return "监控[" + s + "]异常，准备报警";
                }
                return "监控[" + s + "]正常";
            }
        }).print();

        environment.execute();
    }
}
