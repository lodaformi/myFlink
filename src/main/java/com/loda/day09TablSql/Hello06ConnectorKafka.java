package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/19 16:54
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06ConnectorKafka {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //kafka source
        tableEnvironment.executeSql(
                "CREATE TABLE KafkaSourceTable (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `behavior` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_source',\n" +
                        "  'properties.bootstrap.servers' =  'node01:9092, node02:9092, node03:9092',\n" +
                        "  'properties.group.id' = 'lodaSourceGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")"
        );

        //kafka sink
        tableEnvironment.executeSql(
                "CREATE TABLE KafkaSinkTable (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `behavior` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_sink',\n" +
                        "  'properties.bootstrap.servers' = 'node01:9092, node02:9092, node03:9092',\n" +
                        "  'properties.group.id' = 'lodaSinkGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")"
        );

//        tableEnvironment.sqlQuery("select * from KafkaSourceTable").execute().print();

        tableEnvironment.sqlQuery("select * from KafkaSourceTable").insertInto("KafkaSinkTable").execute();

      //  Unsupported SQL query!
//        tableEnvironment.sqlQuery("insert into KafkaSinkTable " +
//                "select * from  KafkaSourceTable").execute();


        //env exec
    }
}
