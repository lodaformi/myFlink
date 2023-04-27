package com.loda.day04State;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/27 17:08
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03StateTestWordCount {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //transformations
        source.flatMap((value, out)->{
            String[] str = value.split("\\s+");
            for (String s : str) {
                out.collect(s);
            }
        },Types.STRING).map(value -> Tuple2.of(value,1), Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(value -> value.f0)
            .sum(1)
            //sink
            .print();

        //env exec
        env.execute();
    }
}
