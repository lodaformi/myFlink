package com.loda.day02EnvSourceTransSink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/16 23:44
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello20UnionOperator {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //source
        DataStreamSource<Integer> source1 = environment.fromElements(1, 3, 5, 7, 9, 11);
        DataStreamSource<Integer> source2 = environment.fromElements(2, 4, 6, 8, 0, 11);
        DataStreamSource<Integer> source3 = environment.fromElements(11, 44, 66, 88, 101, 112);
        DataStreamSource<String> source4 = environment.fromElements("aa", "bb");
        //可以合并多个相同类型的流
        DataStream<Integer> unionStream = source1.union(source2).union(source3);
        //可以合并多个相同类型的流
//        DataStream<Integer> unionStream = source1.union(source2,source3);
        unionStream.print();

        //env exec
        environment.execute();
    }
}
