package com.loda.day09TablSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/19 14:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01APIEnv {
    public static void main(String[] args) {
        //table env method 1
        //创建环境配置【流批可以自己选择】
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        //不支持
//        StreamTableEnvironment streamTableEnvironment1 = StreamTableEnvironment.create(environmentSettings);

        //table env method 2
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);
    }
}
