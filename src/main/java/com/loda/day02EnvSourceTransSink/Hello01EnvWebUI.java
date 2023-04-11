package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/10 15:20
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01EnvWebUI {
    public static void main(String[] args) throws Exception {
        //env
        Configuration configuration = new Configuration();
        configuration.setString(WebOptions.LOG_PATH, "tmp/log/job.log");
        configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, "tmp/log/job.log");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //source
        DataStreamSource<String> stream = environment.socketTextStream("localhost", 9999);

        //transformation
        SingleOutputStreamOperator<String> mapOp = stream.map(word->word.toUpperCase() +"-"+ System.currentTimeMillis());

        //sinkSetting
        KafkaSink<String> sinkSetting = KafkaSink.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flinkSink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        mapOp.sinkTo(sinkSetting);

        //env exec
        environment.execute();
    }
}
