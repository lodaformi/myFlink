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
 * @Date 2023/4/19 15:30
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello04APIDQL {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        DataStreamSource<String> fileSource = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> empSource = fileSource.map(value -> new ObjectMapper().readValue(value, Emp.class));

        //table read from data stream source
        Table empTable = tableEnvironment.fromDataStream(empSource);

        //查询数据：TableAPI--查询20部门员工信息的 编号 姓名 薪资 部门编号
        empTable.filter($("deptno").isEqual(20))
                .select($("empno"), $("ename"), $("sal"), $("deptno"))
                .execute().print();

        //查询：计算各个部门的工资总和；
        // 数据是流式达到，按照数据到达的顺序----滚动计算
        empTable.groupBy($("deptno")).select($("deptno"), $("sal").sum().as("salSum"))
                .execute().print();

        //查询数据：sql--查询20部门员工信息的 编号 姓名 薪资 部门编号
        tableEnvironment.createTemporaryView("t_emp", empTable);
        tableEnvironment.sqlQuery("select empno, ename, sal, deptno from t_emp where deptno = 20")
                .execute().print();

        //查询数据：混合方式--查询20部门员工信息的 编号 姓名 薪资 部门编号
        Table table = tableEnvironment.sqlQuery("select empno, ename, job, deptno from " + empTable.toString() + " where deptno = 20");
        table.filter($("job")
                //大小写敏感
                .isEqual("ANALYST"))
                .select($("ename"), $("job"), $("deptno"))
                .execute().print();

        //transformation

    }
}
