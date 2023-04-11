package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/10 21:18
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello09TransformationIterate {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        environment.setParallelism(1);
        //操作数据
        DataStreamSource<String> source = environment.fromElements("香蕉,51", "苹果,101", "桃子,202");

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> mapStream = source.map(value -> {
            String[] fruitsAndNums = value.split(",");
            return Tuple3.of(fruitsAndNums[0], Integer.valueOf(fruitsAndNums[1]), 0);
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        IterativeStream<Tuple3<String, Integer, Integer>> iterateStream = mapStream.iterate();
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterateBody = iterateStream.map(tuple3 -> {
            tuple3.f1 -= 10;
            tuple3.f2++;
            return tuple3;
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterateFilter = iterateBody.filter(tuple3 -> tuple3.f1 > 10);
        iterateStream.closeWith(iterateFilter);

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterateLeft = iterateBody.filter(tuple3 -> tuple3.f1 <= 10);
        iterateLeft.print("最后剩余：");

        //env exec
        environment.execute();
    }
}
