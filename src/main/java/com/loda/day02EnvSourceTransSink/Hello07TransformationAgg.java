package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/10 21:03
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07TransformationAgg {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        environment.setParallelism(1);
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("math2", 200));
        list.add(new Tuple2<>("chinese2", 20));
        list.add(new Tuple2<>("math1", 100));
        list.add(new Tuple2<>("chinese1", 10));
        list.add(new Tuple2<>("math4", 400));
        list.add(new Tuple2<>("chinese4", 40));
        list.add(new Tuple2<>("math3", 300));
        list.add(new Tuple2<>("chinese3", 30));

        //source
        DataStreamSource<Tuple2<String, Integer>> source = environment.fromCollection(list);

        //transformations
        KeyedStream<Tuple2<String, Integer>, Integer> keyBy = source.keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0.length();
            }
        });

//        keyBy.sum(1).print();
//        keyBy.min(1).print();
//        keyBy.max(1).print();
//        keyBy.minBy(1).print();
        keyBy.maxBy(1).print("maxby-");

        //env exec
        environment.execute();
    }
}
