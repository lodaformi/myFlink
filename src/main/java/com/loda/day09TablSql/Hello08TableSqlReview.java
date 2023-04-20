package com.loda.day09TablSql;

import com.loda.pojo.Emp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author loda
 * @Date 2023/4/20 21:09
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08TableSqlReview {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> empDS = source.map(value -> new ObjectMapper().readValue(value, Emp.class));

        //table
        Table empTable = tableEnvironment.fromDataStream(empDS);
        //sql
        empTable.filter($("deptno").isEqual(10))
                .select($("ename"), $("empno"), $("sal")).execute().print();

//        tableEnvironment.createTemporaryView("t_emp", empDS);
//        tableEnvironment.sqlQuery("select * from t_emp where deptno = 10").execute().print();

        //sink

        //env exec
    }
}
