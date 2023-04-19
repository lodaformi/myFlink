package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/19 17:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07JDBC2Kafka {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //获取SourceTable
        //在 Flink SQL 中注册一张 MySQL 表 userTable
        tableEnvironment.executeSql(
                "CREATE TABLE userTable (\n" +
                        "  user_id INT,\n" +
                        "  behavior STRING,\n" +
                        "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://localhost:3306/scott?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false',\n" +
                        "   'table-name' = 'userBehavior',\n" +
                        "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = '123456'\n" +
                        ");"
        );

        //kafka sink
        tableEnvironment.executeSql(
                "CREATE TABLE KafkaSinkTable (\n" +
                        "  `user_id` INT,\n" +
                        "  `behavior` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_sink',\n" +
                        "  'properties.bootstrap.servers' =  'node01:9092, node02:9092, node03:9092',\n" +
                        "  'properties.group.id' = 'lodaSourceGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")"
        );



        // these two sentences both works well
//        tableEnvironment.sqlQuery("select * from userTable").insertInto("KafkaSinkTable").execute();
        tableEnvironment.executeSql("insert into KafkaSinkTable select * from userTable");
    }
}
