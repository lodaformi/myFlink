package com.loda.day09TablSql;

import com.loda.udf.MyUDFAggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author loda
 * @Date 2023/4/22 19:12
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello27UDFAggregateFunction {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_order (\n" +
                " id INT,\n" +
                " type INT,\n" +
              " weight INT NOT NULL,\n" +
              " price INT NOT NULL\n" +
//                " weight INT,\n" +
//                " price INT\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.id.kind'='sequence',\n" +
                " 'fields.id.start'='1',\n" +
                " 'fields.id.end'='1000',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='3',\n" +
                " 'fields.weight.min'='10',\n" +
                " 'fields.weight.max'='20',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='200'\n" +
                ")");
        //普通查询
        // tableEnvironment.sqlQuery("select * from t_order").execute().print();

        tableEnvironment.executeSql("desc t_order").print();
        /*
            |   name | type | null | key | extras | watermark |
            +--------+------+------+-----+--------+-----------+
            |     id |  INT | TRUE |     |        |           |
            |   type |  INT | TRUE |     |        |           |
            | weight |  INT | TRUE |     |        |           |
            |  price |  INT | TRUE |     |        |           |
            +--------+------+------+-----+--------+-----------+

            +--------+------+-------+-----+--------+-----------+
            |   name | type |  null | key | extras | watermark |
            +--------+------+-------+-----+--------+-----------+
            |     id |  INT |  TRUE |     |        |           |
            |   type |  INT |  TRUE |     |        |           |
            | weight |  INT | FALSE |     |        |           |
            |  price |  INT | FALSE |     |        |           |
            +--------+------+-------+-----+--------+-----------+
         */
        //直接使用
        tableEnvironment.from("t_order")
                .groupBy($("type"))
                .select($("type"),call(MyUDFAggregateFunction.class, $("weight"), $("price")))
                .execute().print();

        // 注册函数
//        tableEnvironment.createTemporarySystemFunction("wAvg", new MyUDFAggregateFunction());

        // 使用函数
        //wAvg(weight => INT NOT NULL, price => INT NOT NULL)
//        tableEnvironment.sqlQuery("SELECT type, wAvg(weight, price) FROM t_order group by type").execute().print();
    }
}
