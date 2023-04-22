package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 17:11
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello16HOP {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //if do not set parallelism, flink will use machine's cpu core to dataGen data
        //for example, cpu cores are 8, rows-per-second = 1, the result is that you will get 8 rows per second
        environment.setParallelism(1);

        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source

        //create table
        TableResult dataGenTable = tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid INT,\n" +
                " sales INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.gid.kind'='sequence',\n" +
                " 'fields.gid.start'='10',\n" +
                " 'fields.gid.end'='100',\n" +
                " 'fields.sales.min'='1',\n" +
                " 'fields.sales.max'='1'\n" +
                ")");

        //按照窗口进行查询--所有信息
        //窗口的滑动步长，窗口的大小
//        tableEnvironment.sqlQuery("select * from table" +
//                        "(HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS))")
//                .execute().print();

//        tableEnvironment.sqlQuery("select window_start, window_end, sum(sales) " +
//                        "from table" +
//                        "(HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS))" +
//                        "GROUP BY window_start, window_end"
//                ).execute().print();

        tableEnvironment.sqlQuery("select gid, sum(sales), window_start, window_end " +
                "from table" +
                "(HOP(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS))" +
                "GROUP BY window_start, window_end, gid"
        ).execute().print();

    }
}
