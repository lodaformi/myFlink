package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/19 17:31
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07ConnectorJDBC {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //获取SourceTable
        //在 Flink SQL 中注册一张 MySQL 表 empTable
        tableEnvironment.executeSql(
                        "CREATE TABLE empTable (\n" +
                        "  empno INT,\n" +
                        "  ename STRING,\n" +
                        "  job STRING,\n" +
                        "  mgr INT,\n" +
                        "  hiredate DATE,\n" +
                        "  sal DECIMAL(10, 2),\n" +
                        "  comm DECIMAL(10, 2),\n" +
                        "  deptno INT,\n" +
                        "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "   'connector' = 'jdbc',\n" +
                        "   'url' = 'jdbc:mysql://localhost:3306/scott?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false',\n" +
                        "   'table-name' = 'emp',\n" +
                        "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "   'username' = 'root',\n" +
                        "   'password' = '123456'\n" +
                        ");"
        );
        tableEnvironment.sqlQuery("select * from empTable").execute().print();

    }
}
