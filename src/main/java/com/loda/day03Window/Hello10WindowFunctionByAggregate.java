package com.loda.day03Window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/12 16:29
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello10WindowFunctionByAggregate {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2->tuple2.f0)
                .countWindow(3)
                //增量计算
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of(" ",0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        accumulator.f0 = accumulator.f0 + "__" + value.f0;
                        accumulator.f1 += value.f1;
//                        System.out.println("Hello10WindowFunctionByAggregate.add"+accumulator);
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return null;
                    }
                }).print("countWindow--Tumbling: ").setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }
}
