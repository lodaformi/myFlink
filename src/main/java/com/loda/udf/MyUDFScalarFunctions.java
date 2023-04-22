package com.loda.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author loda
 * @Date 2023/4/22 17:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MyUDFScalarFunctions extends ScalarFunction {
    public String eval(String s) {
        return s.concat("--"+s.length());
    }
}
