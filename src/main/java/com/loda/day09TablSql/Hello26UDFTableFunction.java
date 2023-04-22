package com.loda.day09TablSql;

import com.loda.udf.MyUDFTableFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 17:31
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello26UDFTableFunction {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_movie (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  types STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- 必选：指定连接器类型\n" +
                "  'path' = 'file:///D:\\Develop\\bigdata\\myFlink01\\data\\movie.txt',  -- 必选：指定路径\n" +
                "  'format' = 'csv'                    -- 必选：文件系统连接器指定 format\n" +
                ")");

//        tableEnvironment.sqlQuery("select * from t_movie").execute().print();

//        tableEnvironment
//                .from("t_movie")
//                .joinLateral(call(MyUDFTableFunction.class, $("types")))
//                .select($("*")).execute().print();

//        tableEnvironment
//                .from("t_movie")
//                .leftOuterJoinLateral(call(MyUDFTableFunction.class, $("types")))
//                .select($("*")).execute().print();

        // 在 Table API 里重命名函数字段
//        tableEnvironment
//                .from("t_movie")
//                .leftOuterJoinLateral(call(MyUDFTableFunction.class, $("types")).as("type", "score"))
//                .select($("*")).execute().print();

        // 注册函数
        tableEnvironment.createTemporarySystemFunction("mySplitFunction", MyUDFTableFunction.class);

// 在 Table API 里调用注册好的函数
//        tableEnvironment
//                .from("t_movie")
//                .joinLateral(call("mySplitFunction", $("types")))
//                .select($("*")).execute().print();

        // 在 SQL 里调用注册好的函数
//        tableEnvironment.sqlQuery(
//                "SELECT * " +
//                        "FROM t_movie, LATERAL TABLE(mySplitFunction(types))").execute().print();


// 在 SQL 里重命名函数字段
        tableEnvironment.sqlQuery(
                "SELECT * " +
                        "FROM t_movie " +
                        "LEFT JOIN LATERAL TABLE(mySplitFunction(types)) AS T(type, score) ON TRUE").execute().print();
    }
}
