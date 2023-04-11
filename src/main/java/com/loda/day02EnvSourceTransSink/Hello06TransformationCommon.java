package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author loda
 * @Date 2023/4/10 20:50
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06TransformationCommon {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        environment.setParallelism(1);
        //source
        DataStreamSource<String> source = environment.fromElements("Hello Hadoop", "Hello Hive", "Hello HBase Phoenix", "Hello       ClickHouse");

        //transformations
        source.flatMap((lines, col)-> Arrays.stream(lines.split("\\s+")).forEach(word->col.collect(word)), Types.STRING)
                .filter(word->word!=null && word.length() > 0)
                .map(word-> Tuple2.of(word,1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();


        //env exec
        environment.execute();
    }
}
