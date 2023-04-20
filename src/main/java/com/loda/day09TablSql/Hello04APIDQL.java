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
        //创建FlinkSql 运行环境
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
//        tableEnvironment.useCatalog();
//        tableEnvironment.useDatabase();

        //source
        DataStreamSource<String> fileSource = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> empSource = fileSource.map(value -> new ObjectMapper().readValue(value, Emp.class));

        //将数据源定义（映射） 成表（视图）
        Table empTable = tableEnvironment.fromDataStream(empSource);

        //执行 sql 语义的查询（sql 语法或者 tableapi）
        //查询数据：TableAPI--查询20部门员工信息的 编号 姓名 薪资 部门编号
        empTable.filter($("deptno").isEqual(20))
                .select($("empno"), $("ename"), $("sal"), $("deptno"));
//                .execute()
//                .print();
//        StreamStatementSet statementSet = tableEnvironment.createStatementSet();
        //add方法的参数是TablePipeline 类型，目前知道insertInto是TablePipeline 类型
//        statementSet.add(empTable.insertInto(""));
//        System.out.println(statementSet.explain());
        System.out.println(empTable.explain());


        //查询：计算各个部门的工资总和；
        // 数据是流式达到，按照数据到达的顺序----滚动计算
//        empTable.groupBy($("deptno")).select($("deptno"), $("sal").sum().as("salSum"))
//                .execute().print();

        //查询数据：sql--查询20部门员工信息的 编号 姓名 薪资 部门编号
//        tableEnvironment.createTemporaryView("t_emp", empTable);
//        tableEnvironment.sqlQuery("select empno, ename, sal, deptno from t_emp where deptno = 20")
//                .execute().print();

        //返回值是TableResult
//        TableResult tableResult = tableEnvironment.executeSql("select empno, ename, sal, deptno from t_emp where deptno = 20");
        //可以直接输出，不用execute
//        tableResult.print();

        //查询数据：混合方式--查询20部门员工信息的 编号 姓名 薪资 部门编号
        //注意：在拼接字符串的时候，前后一定要预留空格（这里的from后，where的前面），否则报错
//        //返回的是table
//        Table table = tableEnvironment.sqlQuery("select empno, ename, job, deptno from " + empTable.toString() + " where deptno = 20");
//        //返回的是table
//        Table filter = table.filter($("job")
//                //大小写敏感
//                .isEqual("ANALYST"));
//        filter
//                .select($("ename"), $("job"), $("deptno"))
//                .execute().print();


    }
}
