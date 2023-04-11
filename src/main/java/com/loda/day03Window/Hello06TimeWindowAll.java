package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author loda
 * @Date 2023/4/11 20:32
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06TimeWindowAll {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformation
//        source.map(word-> Tuple2.of(word.split(":")[0], Integer.valueOf(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce((value1, value2) -> {
//                    value1.f0 = value1.f0 +"__"+ value2.f0;
//                    value1.f1 = value1.f1 + value2.f1;
//                    return value1;
//                })
//                .map(tuple2->{
//                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;
//                    return tuple2;
//                }, Types.TUPLE(Types.STRING, Types.INT))
//                .print("timeWindow--TumblingProcessing: ").setParallelism(1);


//        source.map(word-> Tuple2.of(word.split(":")[0], Integer.valueOf(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
//                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
//                .reduce((value1, value2) -> {
//                    value1.f0 = value1.f0 +"__"+ value2.f0;
//                    value1.f1 = value1.f1 + value2.f1;
//                    return value1;
//                })
//                .map(tuple2->{
//                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;
//                    return tuple2;
//                }, Types.TUPLE(Types.STRING, Types.INT))
//                .print("timeWindow--SlidingProcessing: ").setParallelism(1);

        source.map(word-> Tuple2.of(word.split(":")[0], Integer.valueOf(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce((value1, value2) -> {
                    value1.f0 = value1.f0 +"__"+ value2.f0;
                    value1.f1 = value1.f1 + value2.f1;
                    return value1;
                })
                .map(tuple2->{
                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;
                    return tuple2;
                }, Types.TUPLE(Types.STRING, Types.INT))
                .print("timeWindow--ProcessingTimeSession: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
