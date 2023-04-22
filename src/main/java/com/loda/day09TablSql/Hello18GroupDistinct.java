package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 19:47
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello18GroupDistinct {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //if do not set parallelism, flink will use machine's cpu core to dataGen data
        //for example, cpu cores are 8, rows-per-second = 1, the result is that you will get 8 rows per second
        environment.setParallelism(1);

        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source

        //create table
        TableResult dataGenTable = tableEnvironment.executeSql("CREATE TABLE t_user (\n" +
                " uid INT,\n" +
                " pageView INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" +
                " 'fields.uid.min'='10',\n" +
                " 'fields.uid.max'='110',\n" +
                " 'fields.pageView.min'='1',\n" +
                " 'fields.pageView.max'='5'\n" +
                ")");

        // calc PV(page view) and UV(unique visit)
        tableEnvironment.sqlQuery("select count(pageView) as pv, count(distinct uid) as uv from t_user")
                .execute().print();
    }
}
