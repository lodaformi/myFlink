package com.loda.day03Window;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.*;
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
 * @Date 2023/4/14 17:09
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello17WaterMarkCustomPunctuated {
    public static void main(String[] args) throws Exception {
        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 1000; i < 2000; i++) {
                if (Math.random() <= 0.99) {
                    KafkaUtil.sendMsg("loda", uname + ":" + i + ":" + System.currentTimeMillis());
                } else {
                    KafkaUtil.sendMsg("loda", uname +  i + ":GameOver:" + System.currentTimeMillis());
                }
                try {
                    Thread.sleep(20);
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
                .assignTimestampsAndWatermarks(new MyPunctuatedWaterMarkStratege())
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
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
                })
                .print("timeWindow--TumblingEvent: ");

        //sink

        //env exec
        environment.execute();
    }
}

//事件触发水位线
class MyPunctuatedWaterMarkStratege implements WatermarkStrategy<Tuple3<String, String, Long>> {
    @Override
    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyPunctuatedWaterMarkGenerator();
    }

    private class MyPunctuatedWaterMarkGenerator implements WatermarkGenerator<Tuple3<String, String, Long>> {
        @Override
        public void onEvent(Tuple3<String, String, Long> event, long eventTimestamp, WatermarkOutput output) {
            if ("GameOver".equals(event.f1)) {
                output.emitWatermark(new Watermark(event.f2 - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //什么也不做
        }
    }
}
