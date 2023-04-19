package com.loda.day09TablSql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/19 15:02
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello02APIDDL {
    public static void main(String[] args) {
        //table env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        DataStreamSource<String> source = environment.readTextFile("data/dept.txt");
        //创建表：方案1 流表转换
        Table depTable = tableEnvironment.fromDataStream(source);
        depTable.execute().print();

        //创建表：方案2 TableAPI
//        tableEnvironment.createTable();
//        tableEnvironment.createTemporaryView();

//        tableEnvironment.registerDataStream();

        //创建表：方案3 SQL
//        tableEnvironment.executeSql("create table ...");

    }
}
