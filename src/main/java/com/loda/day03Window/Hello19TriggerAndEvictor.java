package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @Author loda
 * @Date 2023/4/14 20:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello19TriggerAndEvictor {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source-admin:3
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformation
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .evictor(CountEvictor.of(5))
                .reduce((v1, v2)->{
                    v1.f0 = v1.f0 +"__" + v2.f0;
                    v1.f1 += v2.f1;
                    return v1;
                })
                        .print("timeWindow--Sliding: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
