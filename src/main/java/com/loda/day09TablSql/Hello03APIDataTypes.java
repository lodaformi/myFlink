package com.loda.day09TablSql;

import com.loda.pojo.Emp;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author loda
 * @Date 2023/4/19 15:14
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03APIDataTypes {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //table env
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //source
        //Tuple类型
        DataStreamSource<String> source = environment.fromElements("aa,11", "bb,22", "cc,33");
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = source.map(
                value -> Tuple2.of(value.split(",")[0], Integer.parseInt(value.split(",")[1])),
                Types.TUPLE(Types.STRING, Types.INT));

        //default sequence is f0, f1, ...
        //Table tupleTable = tableEnvironment.fromDataStream(streamOperator, $("f0"), $("f1"));
        //sequence can be adjusted using f[x], just like f1, f0, f1 will be consumed firstly
//        Table tupleTable = tableEnvironment.fromDataStream(streamOperator, $("f1"), $("f0"));
        // using as() method to give f[x] a meaningful name, instead of the default name f[x], like f0, f1...
//        Table tupleTable = tableEnvironment.fromDataStream(streamOperator, $("f1").as("name"), $("f0").as("id"));
        //select which part you want from tuple
        Table tupleTable = tableEnvironment.fromDataStream(streamOperator, $("f1").as("name"));
//        tupleTable.execute().print();

        //pojo
        DataStreamSource<String> textFileSource = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> empStream = textFileSource.map(value -> new ObjectMapper().readValue(value, Emp.class));
//        tableEnvironment.fromDataStream(empStream).execute().print();
        tableEnvironment.fromDataStream(empStream, $("empno").as("员工号"), $("ename"), $("job"))
                .execute().print();


        //row
        DataStreamSource<Row> rowDataStreamSource = environment.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
        );
        Table rowTable = tableEnvironment.fromChangelogStream(rowDataStreamSource);
//        rowTable.execute().print();
    }
}
