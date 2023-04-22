package com.loda.day09TablSql;

import org.apache.flink.table.api.*;

/**
 * @Author loda
 * @Date 2023/4/21 15:30
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello11Schema {
    public static void main(String[] args) {
        //env
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        //字段别名必须使用 as；`user_id_new` as user_id * 10
        tableEnvironment.executeSql(
                "CREATE TABLE KafkaSourceTable (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `user_id_new` as user_id * 10,\n" +
                        "  `off` BIGINT METADATA FROM 'offset', --kafka偏移量\n" +
                        "  `record_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp', -- kafka时间戳\n" +
                        "  `behavior` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_source',\n" +
                        "  'properties.bootstrap.servers' =  'node01:9092, node02:9092, node03:9092',\n" +
                        "  'properties.group.id' = 'lodaSourceGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")"
        );

        tableEnvironment.sqlQuery("select * from KafkaSourceTable").execute().print();

        tableEnvironment.createTable("t_user_behavior", TableDescriptor.forConnector("kafka")
                        .schema(Schema.newBuilder()
                                .column("user_id", DataTypes.INT())
                                .columnByExpression("user_id_new", "user_id * 100")
                                .columnByMetadata("off", DataTypes.BIGINT(), "offset", true)
                                .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true)
                                .column("behavior", DataTypes.STRING())
                                .build())
//                        .option("connector", "kafka")
                        .option("topic", "user_source")
                        .option("properties.bootstrap.servers", "node01:9092, node02:9092, node03:9092")
                        .option("properties.group.id", "lodaSourceGroup")
                        .option("scan.startup.mode", "earliest-offset")
                        .format("csv")
                .build());

//        tableEnvironment.sqlQuery("select * from t_user_behavior").execute().print();

    }
}
