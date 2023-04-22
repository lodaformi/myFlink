package com.loda.day09TablSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 16:07
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello12Json {
    public static void main(String[] args) {
        //env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //table env
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);


        tableEnvironment.executeSql(
                "CREATE TABLE t_emp (\n" +
                        "  empno INT,\n" +
                        "  ename STRING,\n" +
                        "  job STRING,\n" +
                        "  mgr INT,\n" +
                        "  hiredate BIGINT,\n" +
                        "  sal DECIMAL(10, 2),\n" +
                        "  comm DECIMAL(10, 2),\n" +
                        "  deptno INT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'filesystem',           -- 必选：指定连接器类型\n" +
                        "  'path' = 'file:///D:/Develop/bigdata/myFlink01/data/emp.txt',  -- 必选：指定路径\n" +
                        "  'format' = 'json'                     -- 必选：文件系统连接器指定 format\n" +
                        ")"
        );
        tableEnvironment.sqlQuery("select * from t_emp").execute().print();
    }
}
