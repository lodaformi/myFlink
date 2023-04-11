package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/4/10 23:29
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello12SinkToKafka {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //多个任务并行时，数据处理的顺序和数据返回的顺序不一定，基本上是乱序的
        environment.setParallelism(2);
        //操作数据
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        KafkaSink<String> sinkSetting = KafkaSink.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flinkSink02")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        source.map(word->word +":"+System.currentTimeMillis()).sinkTo(sinkSetting);

        //env exec
        environment.execute();
    }
}
