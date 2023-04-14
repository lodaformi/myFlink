package com.loda.day03Window;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Locale;

/**
 * @Author loda
 * @Date 2023/4/14 19:45
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello18WaterMarkLateSide {
    public static void main(String[] args) throws Exception {
        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if (i % 10 == 0) {
                    KafkaUtil.sendMsg("loda", uname + ":" + i + ":" + (System.currentTimeMillis() - 15000L));
                } else if (i % 5 == 0) {
                    KafkaUtil.sendMsg("loda", uname + ":" + i + ":" + (System.currentTimeMillis() - 3000L));
                } else {
                    KafkaUtil.sendMsg("loda", uname + ":" + i + ":" + System.currentTimeMillis());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //source
        DataStreamSource<String> source = environment.fromSource(KafkaUtil.getKafkaSource("loda", "lodaKing"), WatermarkStrategy.noWatermarks(), "Kafka Source");

        //sideOutput
        OutputTag<Tuple3<String, String, Long>> outputTag = new OutputTag<>("win10sdur1late5") {
        };

        //transformations
        SingleOutputStreamOperator<String> streamOperator = source.map(value -> Tuple3.of(value.split(":")[0], value.split(":")[1],
                        Long.parseLong(value.split(":")[2])), Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
//                        System.out.println("Hello13WaterMarkInOrder.apply");
                        StringBuffer sb = new StringBuffer();
                        sb.append(" [ " + s + " ] ");
                        for (Tuple3<String, String, Long> tuple3 : input) {
                            sb.append(" [ " + tuple3.f1 + "--" + tuple3.f2 + " ] ");
                        }
                        sb.append(" [ " + window + " ] ");
                        out.collect(sb.toString());
                    }
                });

        //sink
        streamOperator.print("timeWindow--main: ");
        streamOperator.getSideOutput(outputTag).print("timeWindow--sideOutSide: ");

        //env exec
        environment.execute();
    }
}
