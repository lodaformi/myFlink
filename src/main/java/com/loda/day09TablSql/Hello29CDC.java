package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 23:31
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello29CDC {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //创建表
        tableEnvironment.executeSql("CREATE TABLE flink_cdc_dept (\n" +
                "     deptno INT,\n" +
                "     dname STRING,\n" +
                "     loc STRING,\n" +
                "     PRIMARY KEY(deptno) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = '192.168.189.21',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'scott',\n" +
                "     'table-name' = 'dept')");

        //简单查询
        tableEnvironment.sqlQuery("select * from flink_cdc_dept").execute().print();
    }
}
