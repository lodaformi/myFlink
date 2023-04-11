package com.loda.day02EnvSourceTransSink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author loda
 * @Date 2023/4/10 23:01
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello10TransformationUnionConnect {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //多个任务并行时，数据处理的顺序和数据返回的顺序不一定，基本上是乱序的
        environment.setParallelism(2);

//        DataStreamSource<Integer> source1 = environment.fromElements(1, 3, 5, 7, 9);
//        DataStreamSource<Integer> source2 = environment.fromElements(2, 4, 6, 8, 9);
//         //数据类型必须相同
//        source1.union(source2).print();

        //获取数据流--火警传感器[字符串]--温度传感器[数字]
        DataStreamSource<String> source1 = environment.fromElements("yes", "yes", "no", "yes");
        DataStreamSource<Double> source2 = environment.fromElements(33.1, 33.6, 50.2, 33.3);
        ConnectedStreams<String, Double> connectStream = source1.connect(source2);

        connectStream.map(new CoMapFunction<String, Double, String>() {
            @Override
            public String map1(String value) throws Exception {
                if ("no".equals(value)) {
                    return "[" + value + "]火警传感器[危险]";
                }
                return "[" + value + "]火警传感器[安全]";
            }

            @Override
            public String map2(Double value) throws Exception {
                if (50 <= value) {
                    return "[" + value + "]温度传感器[危险]";
                }
                return "[" + value + "]温度传感器[安全]";
            }
        }).print();

        //env exec
        environment.execute();
    }
}
