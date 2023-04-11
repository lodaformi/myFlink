package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/11 20:07
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03CountWindowAll {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformation
//        source.map(word-> Tuple2.of(word.split(":")[0], Integer.valueOf(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
//                .countWindowAll(3)
//                .reduce((value1, value2) -> {
//                    value1.f0 = value1.f0 +"__"+ value2.f0;
//                    value1.f1 = value1.f1 + value2.f1;
//                    return value1;
//                })
//                .print("countWindowAll--Tumbling: ").setParallelism(1);

        source.map(word->Tuple2.of(word.split(":")[0], Integer.valueOf(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .countWindowAll(3,2)
                .reduce((value1, value2) -> {
                    value1.f0 = value1.f0 +"__"+ value2.f0;
                    value1.f1 = value1.f1 + value2.f1;
                    return value1;
                }).print("countWindowAll--sliding: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
