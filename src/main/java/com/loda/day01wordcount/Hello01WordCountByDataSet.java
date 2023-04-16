package com.loda.day01wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/8 17:37
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01WordCountByDataSet {
    public static void main(String[] args) throws Exception {
        //环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //source
        DataSource<String> dataSource = environment.readTextFile("data/wordcount.txt");
        //transfromations
        FlatMapOperator<String, String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        AggregateOperator<Tuple2<String, Integer>> sum = mapOperator.groupBy(0).sum(1);


        //sink
        sum.print();
    }
}
