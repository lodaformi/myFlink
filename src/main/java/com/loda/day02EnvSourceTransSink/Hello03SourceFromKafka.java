package com.loda.day02EnvSourceTransSink;

import com.loda.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/10 16:53
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03SourceFromKafka {
    public static void main(String[] args) throws Exception {
        //创建一个线程持续向Kafka添加数据
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                KafkaUtil.sendMsg("kfkSource", "Hello Flink" + i + "--" + System.currentTimeMillis());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //kafkaSourceSetting
        KafkaSource<String> kafkaSourceSetting = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setTopics("kfkSource")
                .setGroupId("loda-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source = environment.fromSource(kafkaSourceSetting, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //transformations
        source.map(word->word.toUpperCase()).print();

        //sink

        //env exec
        environment.execute();
    }
}
