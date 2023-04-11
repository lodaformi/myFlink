package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author loda
 * @Date 2023/4/11 21:02
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
//暂时无法处理event时间的事件
public class Hello07TimeWindowMinorDefects {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源-admin:3
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //TimeWindow--Tumbling
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                })
                .map(tuple2 -> {
                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;
                    return tuple2;
                }, Types.TUPLE(Types.STRING, Types.INT))
                .print("TimeWindow--Tumbling:").setParallelism(1);

        //运行环境
        environment.execute();
    }
}
