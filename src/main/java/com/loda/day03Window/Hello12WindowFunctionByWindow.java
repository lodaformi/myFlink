package com.loda.day03Window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/12 17:23
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello12WindowFunctionByWindow {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //全量计算
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input,
                                      Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("Hello12WindowFunctionByWindow.apply"+"全量计算"+window);
                        int sum = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            sum += tuple2.f1;
                        }
                        out.collect(Tuple2.of(s, sum));
                    }
                }).print("timeWindow--TumblingProcessing: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
