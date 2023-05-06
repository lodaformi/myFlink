package com.loda.day04State;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author loda
 * @Date 2023/4/15 17:41
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07CoGroup {
    public static void main(String[] args) throws Exception {
        //data
        new Thread(() -> {
            for (int i = 100; i < 500; i++) {
                String goodName = RandomStringUtils.randomAlphabetic(8);
                //模拟数据，4的倍数不生成tGoodsInfo信息
                if (i%4 != 0) {
                    KafkaUtil.sendMsg("tGoodsInfo", goodName + ":info_" + i + ":" + System.currentTimeMillis());
                }
                //模拟数据，5的倍数不生成tGoodsPrice信息
                if (i % 5 != 0) {
                    KafkaUtil.sendMsg("tGoodsPrice", goodName + ":price_" + i + ":" + (System.currentTimeMillis()-3000L));
                }
                try {
                    Thread.sleep(500);
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

        //TumblingEventTimeWindows
        //coGroup相当于全外联
        goodsInfoStream.coGroup(goodsPriceStream)
                .where(goodsInfo -> goodsInfo.f0)
                .equalTo(goodsPrice->goodsPrice.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> first, Iterable<Tuple3<String, String, Long>> second, Collector<String> out) throws Exception {
//                        for (Tuple3<String, String, Long> info : first) {
//                            System.out.println("info [ "+info.toString()+" ]");
//                        }
//                        for (Tuple3<String, String, Long> price : second) {
//                            System.out.println("price [ "+price.toString()+" ]");
//                        }
                        for (Tuple3<String, String, Long> info : first) {
                            for (Tuple3<String, String, Long> price : second) {
//                                if (info.f0.equals(price.f0)) {
                                    System.out.println("+++++++++++++++++++++++++");
                                    System.out.println("price [ "+price.toString()+" ]" + " info [ "+info.toString()+" ]");
                                    System.out.println("+++++++++++++++++++++++++");
//                                }
                            }
                        }

                        //迭代器也可以这样直接输出
//                        System.out.println("2222info [" + first + "] price[" + second + "]");
                        System.out.println("------------------------------------");
                    }
                })
                .print("coGroup--").setParallelism(1);
        //sink

        //env exec
        environment.execute();
    }
}
