package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/11 17:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello18ProcessFunction {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<String> source = environment.readTextFile("data/wordcount.txt");

        //transformations using process function
        //flatmap
        SingleOutputStreamOperator<String> flatMapOp = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                String[] words = value.split("\\s+");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapOp = flatMapOp.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value, 1));
            }
        });

//        mapOp.print();
        mapOp.keyBy(tuple2->tuple2.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("KeyedProcessFunction [ " + ctx.getCurrentKey() +" ]");
                        out.collect(value);
                    }
                }).print();

        //sink

        //env exec
        environment.execute();
    }
}
