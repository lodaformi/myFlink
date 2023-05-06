package com.loda.day04State;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/5/5 8:43
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello10RestartPolicy {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                10,     // 尝试重启的次数
                Time.seconds(10)    //在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
        ));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5,      // 每个时间间隔的最大故障次数
                Time.minutes(10), // 测量故障率的时间间隔
                Time.minutes(1))); // 在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

        //作业直接失败，不尝试重启。
        env.setRestartStrategy(RestartStrategies.noRestart());


        //env exec
        env.execute();
    }
}
