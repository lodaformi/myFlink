package com.loda.day04State;

import com.loda.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author loda
 * @Date 2023/4/15 17:41
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08IntervalJoin {
    public static void main(String[] args) throws Exception {

        //创建2个线程生成数据
        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                //发送goodInfo数据 [id:info:ts]
                KafkaUtil.sendMsg("tGoodsInfo", i + ":info" + i + ":" + System.currentTimeMillis());
                //让线程休眠一下
                try {
                    Thread.sleep((int) (Math.random() * 3000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                //创建goodPrice数据[id:price:ts]
                KafkaUtil.sendMsg("tGoodsPrice", i + ":" + i + ":" + System.currentTimeMillis());
                //让线程休眠一下
                try {
                    Thread.sleep((int) (Math.random() * 3000));
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

        // intervalJoin只能用到keyby的算子
        //而且没有窗口的概念
        goodsInfoStream.keyBy(value -> value.f0)
                .intervalJoin(goodsPriceStream.keyBy(value -> value.f0))
                .between(Time.seconds(-3), Time.seconds(3))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("info [" + left + "] price[" + right + "] context [" + ctx + "]");
                    }
                }).print().setParallelism(2);

        //env exec
        environment.execute();
    }
}
