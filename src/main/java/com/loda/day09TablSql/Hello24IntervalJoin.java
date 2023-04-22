package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 17:10
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello24IntervalJoin {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid STRING,\n" +
                " type INT,\n" +
                " price INT,\n" +
                " ts1 AS localtimestamp,\n" +
                " WATERMARK FOR ts1 AS ts1 - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" +
                " 'fields.gid.length'='10',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='999',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE t_types (\n" +
                " type INT,\n" +
                " tname STRING,\n" +
                " ts2 AS localtimestamp,\n" +
                " WATERMARK FOR ts2 AS ts2 - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" +
                " 'fields.type.kind'='sequence',\n" +
                " 'fields.type.start'='1',\n" +
                " 'fields.type.end'='1000',\n" +
                " 'fields.tname.length'='10'\n" +
                ")");


        tableEnvironment.sqlQuery("select * from t_goods tg , t_types tt where tg.type = tt.type" +
                " AND tg.ts1 BETWEEN tt.ts2 - INTERVAL '4' SECONDS AND tt.ts2 + INTERVAL '4' SECONDS ").execute().print();

    }
}
