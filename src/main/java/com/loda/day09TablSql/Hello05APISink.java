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
 * @Date 2023/4/19 16:19
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05APISink {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        DataStreamSource<String> fileSource = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> empStream = fileSource.map(value -> new ObjectMapper().readValue(value, Emp.class));
        //Pojo类型
        Table empTable = tableEnvironment.fromDataStream(empStream);

        //TableApi查询的结果为table
        Table table1 = empTable.select($("empno"), $("ename"));
        //SQL查询的结果为table
        Table table2 = tableEnvironment.sqlQuery("select empno, ename, job from " + empTable.toString());

//        tableEnvironment.toDataStream(table1).print();

        tableEnvironment.executeSql(
                "CREATE TABLE print_table (\n" +
                        " empno INT,\n" +
                        " ename STRING,\n" +
                        " job STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")"
        );
        table2.insertInto("print_table").execute();

        //env exec
        environment.execute();
    }
}
