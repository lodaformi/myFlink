package com.loda.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/8 21:46
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06WordCountByDataStream {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 9999);

        //transformations
        SingleOutputStreamOperator<Tuple2<Object, Integer>> sum = streamSource.flatMap((value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }).map(value -> Tuple2.of(value, 1))
                .keyBy(value -> value.f0)
                .sum(1);
        //sink
        sum.print();

        //execute
        environment.execute();
    }
}
