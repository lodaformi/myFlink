package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/10 21:13
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08TransformationReduce {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<String> source = environment.fromElements("a", "b", "bb", "c", "d", "aa");
//        DataSource<String> source = environment.fromElements("a", "b", "bb", "c", "d", "aa");
//        source.groupBy(word->word.length()).reduce((val1, val2)->val1 + "#" + val2).print();
        source.keyBy(word->word.length()).reduce((val1, val2)->val1 + "#" + val2).print();

        //能不能实现min，max，sum，minby，maxby？？？

        //env exec
        environment.execute();
    }
}
