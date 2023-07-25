package com.loda.day04State;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @Author loda
 * @Date 2023/4/14 20:41
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03StateKeyed {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //指定从哪个ckpt恢复数据
        configuration.setString("execution.savepoint.path", "D:\\Develop\\bigdata\\myFlink01\\ckpt\\45389ee89c1c9bf83558403ae0dcf8c1\\chk-13");

        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setParallelism(1);
        //5秒中保存一次checkpoint
        environment.enableCheckpointing(5000);
        //指定ckpt路径
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //获取数据源【水果:重量】
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //计算
        source.map(line -> {
                    String[] split = line.split(":");
                    return Tuple2.of(split[0], Integer.parseInt(split[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .reduce(new YjxxtKeyedStateFunction())
                .print();
        //运行环境
        environment.execute();
    }
}

/**
 * 有钱可以为所欲为
 */
class YjxxtKeyedStateFunction extends RichReduceFunction<Tuple2<String, Integer>> {
    //声明一个状态对象
    private ValueState<Tuple2<String, Integer>> valueState;

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        //开始计算
        value1.f0 = value1.f0 + "__" + value2.f0;
        value1.f1 = value1.f1 + value2.f1;

        //保存状态[自己动手丰衣足食]
        valueState.update(value1);

        return value1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化状态对象
        ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("reduceValueState", Types.TUPLE(Types.STRING, Types.INT));
        this.valueState = getRuntimeContext().getState(descriptor);
    }
}
