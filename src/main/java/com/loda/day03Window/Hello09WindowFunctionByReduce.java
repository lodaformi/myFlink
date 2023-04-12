package com.loda.day03Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/12 16:13
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello09WindowFunctionByReduce {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        //增量计算
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2->tuple2.f0)
                .countWindow(3)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("Hello09WindowFunctionByReduce.reduce 来一条v1[ "+value1+" ] v2 [ "+value2+" ]处理一条");
                        value1.f0 = value1.f0 +"__"+value2.f0;
                        value1.f1 = value1.f1 + value2.f1;
                        return value1;
                    }
                }).print("countWindow--Tumbling: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
