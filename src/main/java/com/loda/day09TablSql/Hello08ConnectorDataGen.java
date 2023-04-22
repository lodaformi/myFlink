package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 14:59
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08ConnectorDataGen {
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
        TableResult dataGenTable = tableEnvironment.executeSql("CREATE TABLE t_datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " -- optional options --\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='1000',\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");

        //sql
        tableEnvironment.sqlQuery("select * from t_datagen").execute().print();

        //sink

        //env exec
    }
}
