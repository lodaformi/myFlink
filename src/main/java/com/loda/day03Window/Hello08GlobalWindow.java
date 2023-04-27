package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

/**
 * @Author loda
 * @Date 2023/4/11 21:06
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08GlobalWindow {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源-admin:3
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //GlobalWindow -- keyed
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .reduce((value1, value2) -> {
                    value1.f0 = value1.f0 + "__" + value2.f0;
                    value1.f1 = value1.f1 + value2.f1;
                    return value1;
                }).print("GlobalWindows: ").setParallelism(1);

        // no-keyed
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .windowAll(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .reduce((value1, value2) -> {
                    value1.f0 = value1.f0 +"__"+ value2.f0;
                    value1.f1 = value1.f1 + value2.f1;
                    return value1;
                }).print("GlobalWindowAll: ").setParallelism(2);
        //运行环境
        environment.execute();
    }
}
