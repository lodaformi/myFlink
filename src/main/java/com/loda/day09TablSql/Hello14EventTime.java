package com.loda.day09TablSql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author loda
 * @Date 2023/4/21 16:46
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello14EventTime {
    public static void main(String[] args) {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        DataStreamSource<String> source1 = environment.fromElements("zhangsan:" + (System.currentTimeMillis()-100));
        SingleOutputStreamOperator<String> dStream = source1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(":")[1]);
                    }
                }));
        Table table = tableEnvironment.fromDataStream(dStream, $("name"), $("eventTime").rowtime());
//        tableEnvironment.sqlQuery("select * from " + table.toString()).execute().print();


        DataStreamSource<String> source2 = environment.fromElements("zhangsan:" + (System.currentTimeMillis()-100));
        SingleOutputStreamOperator<Tuple2<String, Long>> dStream2 = source2.map(value -> Tuple2.of(value.split(":")[0], Long.parseLong(value.split(":")[1]))
                , Types.TUPLE(Types.STRING, Types.LONG));

        Table table2 = tableEnvironment.fromDataStream(dStream2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.BIGINT())
                .columnByExpression("pt", "now()")
                .columnByExpression("et", "TO_TIMESTAMP(FROM_UNIXTIME(f1/1000))")
                .watermark("et", "et - INTERVAL '5' SECOND")
                .build());
        tableEnvironment.sqlQuery("select * from " + table2.toString()).execute().print();
    }
}
