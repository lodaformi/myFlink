package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/4/10 23:12
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello11SinkCommon {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //多个任务并行时，数据处理的顺序和数据返回的顺序不一定，基本上是乱序的
        environment.setParallelism(5);

        //操作数据
        DataStreamSource<Integer> source = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
//        source.print();
        //1个并行度时，直接写到文件中
        //多个并行度时，创建文件夹，每个task创建一个文件，存放分配到的数据
        source.writeAsText("data/text_"+System.currentTimeMillis());

        //csv必须处理tuple的数据The writeAsCsv() method can only be used on data streams of tuples.
//        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapStream = source
//                .map(value -> Tuple2.of(value, (int) (Math.random() * 1000 + 5)), Types.TUPLE(Types.INT, Types.INT));
//        mapStream.writeAsCsv("data/text_"+System.currentTimeMillis());

        DataStreamSink<Integer> toSocket = source.writeToSocket("localhost", 9999, new SerializationSchema<Integer>() {
            @Override
            public byte[] serialize(Integer element) {
                return (element + " ").getBytes(StandardCharsets.UTF_8);
            }
        });


        //env exec
        environment.execute();
    }
}
