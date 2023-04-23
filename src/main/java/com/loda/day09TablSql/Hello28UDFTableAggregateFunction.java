package com.loda.day09TablSql;

import com.loda.udf.MyUDFTableAggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/22 23:21
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello28UDFTableAggregateFunction {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_order (\n" +
                " id INT,\n" +
                " type INT,\n" +
                " price INT\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.id.kind'='sequence',\n" +
                " 'fields.id.start'='1',\n" +
                " 'fields.id.end'='1000',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='3',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='200'\n" +
                ")");

        //普通查询
        // tableEnvironment.sqlQuery("select * from t_order").execute().print();

        //直接使用，用不了，怎么回事？？？
//        tableEnvironment.from("t_order")
//                .groupBy($("type"))
//                .select($("type"), call(MyUDFTableAggregateFunction.class, $("price")))
//                .execute().print();

        tableEnvironment.createTemporarySystemFunction("top2", new MyUDFTableAggregateFunction());

        // 使用函数
        tableEnvironment.sqlQuery("select type,top2(price) from t_order group by type")
                .execute().print();
    }
}
