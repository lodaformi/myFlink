package com.loda.day03Window;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @Author loda
 * @Date 2023/4/12 20:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello15WaterMarkCustomPeriodInOrder02 {
    public static void main(String[] args) throws Exception {
        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 1000; i < 2000; i++) {
                KafkaUtil.sendMsg("loda", uname + ":" + i + ":" + System.currentTimeMillis());
                try {
                    Thread.sleep(50);
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

        //transformations
        source.map(value -> Tuple3.of(value.split(":")[0], value.split(":")[1],
                        Long.parseLong(value.split(":")[2])), Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new MyPeriodicWaterMarkStrategy02())
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                                System.out.println("Hello15WaterMarkCustomPeriodInOrder02.reduce value1 ["+value1.f2+"] + value2["+value2.f2+"]");
                                value1.f0 = value1.f0 +"__" +value2.f0;
                                value1.f1 += value2.f1;
                                return value1;
                    }
                })
                .print("timeWindow--TumblingEvent: ");

        //sink

        //env exec
        environment.execute();
    }
}

//<Tuple3<String, String, Long>>传入数据的泛型
class MyPeriodicWaterMarkStrategy02 implements WatermarkStrategy<Tuple3<String, String, Long>> {
    @Override
    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyPeriodicWaterMarkGenerator();
    }

    /**
     * 自定义水位线生成器[有序]
     */
    private class  MyPeriodicWaterMarkGenerator implements WatermarkGenerator<Tuple3<String, String, Long>> {
        private Long MAX_TS = Long.MIN_VALUE;

        //每个事件都要触发一次onEvent函数
        @Override
        public void onEvent(Tuple3<String, String, Long> event, long eventTimestamp, WatermarkOutput output) {
            MAX_TS = event.f2;
             System.out.println("当前[" + event + "][" + MAX_TS + "]");
        }

        //周期发射watermark，默认为200ms发射一次
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyPeriodicWaterMarkGenerator.onPeriodicEmit ["+MAX_TS+"]");
            output.emitWatermark(new Watermark(MAX_TS-1L));
        }
    }
}
