package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 16:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello22TopNWindow {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //sql
        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid STRING,\n" +
                " type INT,\n" +
                " price INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" +
                " 'fields.gid.length'='10',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='5',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999'\n" +
                ")");
        //check
//        tableEnvironment.sqlQuery("select * from t_goods").execute().print();

        // tumble window
//        tableEnvironment.sqlQuery("select * from table ( " +
//                        "TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS))")
//                .execute().print();

        //查询5秒内 每个种类销售价格最高的前三名
        //get top3 price in one type of all types
        tableEnvironment.sqlQuery("select * from ( " +
                            "select *, " +
                            "ROW_NUMBER() " +
                            "OVER(PARTITION BY window_start, window_end " +
                            "ORDER BY price desc) AS RNum " +
                            "from table( " +
                                    "TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS)) " +
                            ") " +
                        "where RNum <= 3")
                .execute().print();

        //查询5秒内 销售额最高的前三种类
        //get top3 total sales price all types
//        tableEnvironment.sqlQuery("select * from ( " +
//                        "select *, " +
//                            "ROW_NUMBER() " +
//                            "OVER(PARTITION BY window_start, window_end " +
//                            "ORDER BY sp desc) AS RNum " +
//                            "from ( " +
//                                "select type, window_start, window_end, SUM(price) as sp from table ( " +
//                                "TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS)) " +
//                                "GROUP BY type, window_start, window_end" +
//                            " )" +
//                        ") " +
//                        "where RNum <= 3")
//                .execute().print();


    }
}
