package com.loda.day03Window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/12 16:49
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello11WindowFunctionByAggregateAvgPlus {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2->tuple2.f0)
                .countWindow(3)
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        return Tuple3.of(null, 0, 0);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> value, Tuple3<String, Integer, Integer> accumulator) {
                        System.out.println("Hello11WindowFunctionByAggregateAvgPlus.add 来一条v1[ "+value+" ] acc [ "+accumulator+" ]处理一条");
                        //key
                        accumulator.f0 = value.f0;
                        // 累加和
                        accumulator.f1 += value.f1;
                        //累加次数
                        accumulator.f2 ++;
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Integer, Integer> accumulator) {
                        return accumulator.f2 == 0 ? Tuple2.of(accumulator.f0, 0.0)
                                : Tuple2.of(accumulator.f0, accumulator.f1 * 1.0 /accumulator.f2);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
                        return null;
                    }
                })
                .print("countWindow--Tumbling: ").setParallelism(1);
        
        //sink
        
        //env exec
        environment.execute();
    }
}
