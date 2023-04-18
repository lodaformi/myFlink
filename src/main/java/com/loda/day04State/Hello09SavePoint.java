package com.loda.day04State;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author loda
 * @Date 2023/4/17 23:35
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello09SavePoint {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(5);
        //source
        // [word1 word2 word4]
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.flatMap((value, out) -> Arrays.stream(value.split("\\s+")).forEach(out::collect), Types.STRING)
                .map(value -> Tuple2.of(value, 1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);


        operator.print();

        //sink

        //env exec
        environment.execute("Hello01WordCount--"+System.currentTimeMillis());
    }
}
