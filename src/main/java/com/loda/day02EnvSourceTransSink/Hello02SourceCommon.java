package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * @Author loda
 * @Date 2023/4/10 16:22
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello02SourceCommon {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        environment.setParallelism(1);
        //source
        DataStreamSource<String> source1 = environment.readTextFile("data/test.txt");
//        DataStreamSource<String> source2 = environment.socketTextStream("localhost", 9999);

        DataStreamSource<String> source3 = environment.fromCollection(List.of("a", "b", "c", "dD", "Eof"));
        DataStreamSource<String> source = environment.fromElements("a", "b", "c", "dD", "Eof");

        //transformations
        //file transformations
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source1.flatMap((lines, collector) ->
//                Arrays.stream(lines.split("\\s+")).forEach(collector::collect), Types.STRING)
//                .filter(word -> word != null && word.length() > 0)
//                .map(word -> Tuple2.of(word, 1), Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(value -> value.f0)
//                .sum(1);
        //sink
//        sum.print();

        //collection transformations + sink
        source.map(word->word.toUpperCase()).print();



        //env exec
        environment.execute();
    }
}
