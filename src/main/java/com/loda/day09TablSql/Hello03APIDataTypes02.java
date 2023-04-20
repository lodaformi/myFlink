package com.loda.day09TablSql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @Author loda
 * @Date 2023/4/19 15:14
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03APIDataTypes02 {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        //Tuple类型
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");
        Table deptTable = tableEnvironment.fromDataStream(source, $("f0"));

        System.out.println(deptTable.explain());
//        deptTable.execute().print();

    }
}
