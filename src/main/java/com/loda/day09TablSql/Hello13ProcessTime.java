package com.loda.day09TablSql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author loda
 * @Date 2023/4/21 16:30
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello13ProcessTime {
    public static void main(String[] args) {
        //env
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //method 1
        DataStreamSource<String> source = environment.readTextFile("data/dept.txt");
        Table deptTable = tableEnvironment.fromDataStream(source, $("str"), $("pt").proctime());
//        tableEnvironment.sqlQuery("select * from " + deptTable.toString()).execute().print();

        //method 2
        DataStreamSource<String> source2 = environment.readTextFile("data/dept.txt");
        Table table2 = tableEnvironment.fromDataStream(source2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .build());
        tableEnvironment.sqlQuery("select * from " + table2.toString()).execute().print();
    }
}
