package com.loda.day02EnvSourceTransSink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author loda
 * @Date 2023/4/27 18:56
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello22ConnectOperator02 {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<Integer> source1 = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<Integer> source2 = env.fromElements(2, 4, 6, 8, 9);
        //transformations
        source1.connect(source2).map(new CoMapFunction<Integer, Integer, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                if (value % 2 != 0){
                    return value;
                }
                return -1;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                if (value % 2 == 0){
                    return value;
                }
                return -2;
            }
        }).print();


        //sink

        //env exec
        env.execute();
    }
}
