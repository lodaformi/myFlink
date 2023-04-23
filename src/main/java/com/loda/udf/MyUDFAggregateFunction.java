package com.loda.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author loda
 * @Date 2023/4/22 23:03
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MyUDFAggregateFunction extends AggregateFunction<Double, Tuple2<Integer,Integer>> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        //Tuple2.of(总销售额, 总重量)
        return Tuple2.of(0,0);
    }

    //默认情况下，flink接收的不能为null的参数，例如：weight => INT NOT NULL
    //在使用TableAPI和sql进行传参时，传入的可能是允许为null的值，所以此处是更改参数的默认设置，
    //或者在数据源限制，例如INT NOT NULL，两种方式灵活运用
    @FunctionHint(
            input = {@DataTypeHint("INT"), @DataTypeHint("INT")}
    )
    public void accumulate(Tuple2<Integer,Integer> acc, int weight, int price) {
        acc.f0 += weight * price;
        acc.f1 += weight;
    }

    @Override
    public Double getValue(Tuple2<Integer, Integer> accumulator) {
        if (accumulator.f1 != 0) {
            return accumulator.f0 * 1.0 /accumulator.f1;
        }
        return 0.0;
    }
}
