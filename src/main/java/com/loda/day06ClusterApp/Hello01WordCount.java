package com.loda.day06ClusterApp;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author loda
 * @Date 2023/4/17 16:10
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01WordCount {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        // [word1 word2 word4]
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
//        source.flatMap((value, out) -> {
//            String[] split = value.split("\\s+");
//            out.collect(split);
//        });
        source.flatMap((value, out) -> Arrays.stream(value.split("\\s+")).forEach(out::collect), Types.STRING)
                .map(value -> Tuple2.of(value, 1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        //sink

        //env exec
        environment.execute("Hello01WordCount--"+System.currentTimeMillis());
    }
}
