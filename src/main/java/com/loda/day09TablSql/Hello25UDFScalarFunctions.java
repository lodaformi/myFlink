package com.loda.day09TablSql;

import com.loda.udf.MyUDFScalarFunctions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 17:19
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello25UDFScalarFunctions {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='1000',\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");

        // 在 Table API 里不经注册直接“内联”调用函数
//        tableEnvironment.from("t_datagen")
//                .select(call(MyUDFScalarFunctions.class, $("f_random_str")))
//                .execute().print();

        // 注册函数
        tableEnvironment.createTemporarySystemFunction("strConcat", MyUDFScalarFunctions.class);

        // 在 Table API 里调用注册好的函数
//        tableEnvironment.from("t_datagen")
//                .select(call("strConcat", $("f_random_str")))
//                .execute().print();

        // 在 SQL 里调用注册好的函数
        tableEnvironment.sqlQuery("SELECT strConcat(f_random_str) FROM t_datagen").execute().print();
    }
}
