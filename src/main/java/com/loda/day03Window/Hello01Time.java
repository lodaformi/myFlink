package com.loda.day03Window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author loda
 * @Date 2023/4/11 19:55
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01Time {
    public static void main(String[] args) throws Exception{
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.12以前--给所有的流设置时间语义
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        environment.setParallelism(TimeCharacteristic.ProcessingTime);

        //source
        DataStreamSource<String> source = environment.fromElements("aa", "bb", "cc");

        //transformations
        source.keyBy(w->w).window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //sink

        //env exec
        environment.execute();
    }
}
