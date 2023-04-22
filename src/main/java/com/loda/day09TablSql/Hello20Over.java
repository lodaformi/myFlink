package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 20:15
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello20Over {
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
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.gid.length'='10',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='5',\n" +
                " 'fields.price.min'='1',\n" +
                " 'fields.price.max'='9'\n" +
                ")");

        //开窗计算--时间范围
//        tableEnvironment.sqlQuery("select t.*, avg(price) OVER(" +
//                                "PARTITION BY type " +
//                                "ORDER BY ts " +
//                                "RANGE BETWEEN INTERVAL '10' SECONDS PRECEDING AND CURRENT ROW)" +
//                                " from t_goods t "
//                ).execute().print();

        //开窗计算--计数范围
        tableEnvironment.sqlQuery("select *, avg(price)  OVER( " +
                "PARTITION BY type " +
                "ORDER BY ts " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
                "from t_goods").execute().print();
    }
}
