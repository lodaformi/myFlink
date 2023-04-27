package com.loda.day04State;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author loda
 * @Date 2023/4/15 17:40
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06Join {
    public static void main(String[] args) throws Exception {
        //data
        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                String goodName = RandomStringUtils.randomAlphabetic(8);
                KafkaUtil.sendMsg("tGoodsInfo", goodName + ":info_" + i + ":" + System.currentTimeMillis());
                KafkaUtil.sendMsg("tGoodsPrice", goodName + ":price_" + i + ":" + (System.currentTimeMillis() - 3000L));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        environment.setParallelism(2);

        //source
        DataStreamSource<String> goodsInfoSource = environment.fromSource(KafkaUtil.getKafkaSource("tGoodsInfo", "lodaKing"), WatermarkStrategy.noWatermarks(), "tGoodsInfo");
        DataStreamSource<String> goodsPriceSource = environment.fromSource(KafkaUtil.getKafkaSource("tGoodsPrice", "lodaKing"), WatermarkStrategy.noWatermarks(), "tGoodsPrice");

        //transformations
        SingleOutputStreamOperator<Tuple3<String, String, Long>> goodsInfoStream = goodsInfoSource.map(value -> {
                    String[] strings = value.split(":");
                    return Tuple3.of(strings[0], strings[1], Long.parseLong(strings[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> goodsPriceStream = goodsPriceSource.map(value -> {
                    String[] strings = value.split(":");
                    return Tuple3.of(strings[0], strings[1], Long.parseLong(strings[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        //TumblingEventTimeWindows全量窗口计算apply，只有apply
//        goodsInfoStream.join(goodsPriceStream)
//                //第一个流的字段
//                .where(goodsInfo -> goodsInfo.f0)
//                //匹配第二个流的字段
//                .equalTo(goodsPrice -> goodsPrice.f0)
//                //两个流中的数据在同一个窗口中才能匹配到
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
//                    @Override
//                    public String join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
//                        return "info [" + first + "] price[" + second + "]";
//                    }
//                }).print("TumblingEventTimeWindows");

        //SlidingEventTimeWindows
//        goodsInfoStream.join(goodsPriceStream)
//                .where(goodsInfo -> goodsInfo.f0)
//                .equalTo(goodsPrice -> goodsPrice.f0)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
//                    @Override
//                    public String join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
//                        return "info [" + first + "] price[" + second + "]";
//                    }
//                }).print("SlidingEventTimeWindows--");

        goodsInfoStream.join(goodsPriceStream)
                .where(goodsInfo -> goodsInfo.f0)
                .equalTo(goodsPrice -> goodsPrice.f0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public String join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
                        return "info [" + first + "] price[" + second + "]";
                    }
                }).print("sessionWindow--");

        //sink

        //env exec
        environment.execute();
    }
}
