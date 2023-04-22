package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 15:55
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello21TopN {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //env 1
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment1 = TableEnvironment.create(settings);

        //sql
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
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999'\n" +
                ")");
        //check
//        tableEnvironment.sqlQuery("select * from t_goods").execute().print();

        //排序开窗函数--对所有的数据排序
        //get the top 3 price of goods in one type of all types
        tableEnvironment.sqlQuery("select * from (" +
                            "select *, " +
                            "ROW_NUMBER() OVER (PARTITION BY type ORDER BY price desc) AS RNum " +
                            "from t_goods " +
                        ") where RNum <= 3" )
        .execute().print();

    }
}
