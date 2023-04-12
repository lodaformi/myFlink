package com.loda.day03Window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/12 16:49
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello10WindowFunctionByAggregateAvg {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //transformations
        source.map(value -> Tuple2.of(value.split(":")[0], Integer.parseInt(value.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2->tuple2.f0)
                .countWindow(3)
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
                        accumulator.f0 += value.f1;
                        accumulator.f1 ++;
                        return accumulator;
                    }

                    @Override
                    public  Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f1 == 0 ? 0L : accumulator.f0 * 1.0/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return null;
                    }
                }).print("countWindow--Tumbling: ").setParallelism(1);
        
        //sink
        
        //env exec
        environment.execute();
    }
}
