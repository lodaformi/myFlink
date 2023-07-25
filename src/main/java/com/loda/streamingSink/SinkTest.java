package com.loda.streamingSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Author loda
 * @Date 2023/7/6 11:05
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class SinkTest {
    public static void main(String[] args) throws Exception {
        //        //配置程序执行的参数，指定从哪个checkpoint恢复
        //        //如果是本地，就找本地的路径，如果是hdfs路径，就要指定hdfs上的路径
//        Configuration configuration = new Configuration();
//        configuration.setString("execution.savepoint.path", "D:\\Develop\\bigdata\\myFlink01\\ckpt\\45d63ba341c699d99019395444063f16\\chk-40");

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度的设置有很大关系，影响实时拉取数据的速度
        //文件以"前缀-并行度-counter-后缀"命名
        env.setParallelism(2);

        //开启checkpoint
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");

        //创建一个线程持续向Kafka添加数据
//        new Thread(() -> {
//            for (int i = 3000; i < 5000; i++) {
//                KafkaUtil.sendMsg("kfkSource", "Hello Flink" + i + "--" + System.currentTimeMillis());
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }).start();

        //kafkaSourceSetting
        KafkaSource<String> kafkaSourceSetting = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setTopics("kfkSource")
                .setGroupId("loda-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

//        environment.addSource();
//        environment.fromSource()
        DataStreamSource<String> source = env.fromSource(kafkaSourceSetting,
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        //transformations

        //source
//        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //transformation
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();

        //sink
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("D:\\Develop\\bigdata\\myFlink01\\sinkDir"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    //以BucketId创建文件夹
                    @Override
                    public String getBucketId(String element, Context context) {
                        //将时间戳格式化为-> 日期-小时
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd-HH");
                        String s = element.split("--")[1];
                        return formatter.format(LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(Long.parseLong(s)), ZoneId.systemDefault()));
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return new SimpleVersionedSerializer<String>() {
                            @Override
                            public int getVersion() {
                                return 0;
                            }

                            @Override
                            public byte[] serialize(String obj) throws IOException {
                                return new byte[0];
                            }

                            @Override
                            public String deserialize(int version, byte[] serialized) throws IOException {
                                return null;
                            }
                        };
                    }
                })
                .withRollingPolicy(
//                      OnCheckpointRollingPolicy.build()
                        DefaultRollingPolicy.builder()
                                //至少包含 10s 的数据
                            .withRolloverInterval(Duration.ofSeconds(10))
                                //最近 5s 没有收到新的记录
                            .withInactivityInterval(Duration.ofSeconds(5))
                            .withMaxPartSize(new MemorySize(5*1024))
                        .build()
                )
                .withOutputFileConfig(config)
                .build();

//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withMaxPartSize(2* 1024)
//                                .withRolloverInterval(Duration.ofSeconds(10))
//                                .withInactivityInterval(Duration.ofSeconds(5))
//                                .build())
//                .build();
//        source.addSink(sink);
        source
            .map(word->word.toUpperCase())
            .addSink(sink);

        //env exec
        env.execute();
    }
}
