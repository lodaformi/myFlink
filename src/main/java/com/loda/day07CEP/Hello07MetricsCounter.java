package com.loda.day07CEP;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author loda
 * @Date 2023/4/18 21:09
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07MetricsCounter {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //transformations
        //Source [word word word]
        environment.socketTextStream("localhost", 9999)
                .flatMap((line, collector) -> Arrays.stream(line.split(" ")).forEach(collector::collect), Types.STRING)
                .map(new MyCounter())
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                //sink
                .print();

        //env exec
        environment.execute("Hello07MetricsCounter" + System.currentTimeMillis());
    }
}

class MyCounter extends RichMapFunction<String, Tuple2<String, Integer>> {

    private transient Counter counter;
//    Histogram
//    Meter
//    Gauge

    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        if (value != null && value.length() == 0) {
            this.counter.inc();
        }
        return Tuple2.of(value,1);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Creates a new MetricGroup and adds it to this groups sub-groups.
        this.counter = getRuntimeContext().getMetricGroup().addGroup("lodaCs").counter("Len0Counter");

        //Creates a new key-value MetricGroup pair.
        // The key group is added to this groups sub-groups, while the value
        // group is added to the key group's sub-groups.
        // This method returns the value group.
        //this.counter = getRuntimeContext().getMetricGroup().addGroup("lodaCs", "wewe").counter("Len0Counter");
    }
}
