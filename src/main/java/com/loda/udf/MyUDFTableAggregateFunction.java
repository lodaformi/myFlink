package com.loda.udf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @Author loda
 * @Date 2023/4/22 19:13
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MyUDFTableAggregateFunction extends TableAggregateFunction<String, Tuple3<Integer, Integer, Boolean>> {

    @Override
    public Tuple3<Integer, Integer, Boolean> createAccumulator() {
        return Tuple3.of(0, 0, false);
    }

    public void accumulate(Tuple3<Integer, Integer, Boolean> acc, Integer iValue) {
        if (iValue > acc.f0) {
            acc.f2 = true;
            acc.f1 = acc.f0;
            acc.f0 = iValue;
        } else if (iValue > acc.f1) {
            acc.f2 = true;
            acc.f1 = iValue;
        } else {
            acc.f2 = false;
        }
    }

    public void emitValue(Tuple3<Integer, Integer, Boolean> acc, Collector<String> out) {
        if (acc.f2) {
            acc.f2 = false;
            out.collect("First[ " + acc.f0 + " ], Second[ " + acc.f1 + " ]");
        }
    }


}
