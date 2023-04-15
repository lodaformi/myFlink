package com.loda.day04State;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/14 20:41
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05StateTTL {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.enableCheckpointing(5000);
        //本地状态维护
        environment.setStateBackend(new EmbeddedRocksDBStateBackend());
        //远程状态备份
        environment.getCheckpointConfig().setCheckpointStorage("hdfs://node02:8020/flink/checkpoints");
        //获取Source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //转换数据
        source.map(new MyStateBackend()).print();
        //运行环境
        environment.execute();
    }
}

class MyTTL implements MapFunction<String, String>, CheckpointedFunction {
    //声明变量计数器
    private int count;
    //状态对象
    private ListState<Integer> listState;

    @Override
    public String map(String value) throws Exception {
        //计数器累加
        count++;
        return "[" + value.toUpperCase() + "][" + count + "]";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //清空并更新数据
        listState.clear();
        listState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                //更新策略
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                //失效状态的可见性
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .cleanupFullSnapshot()
                .build();
        //创建描述器并创建对象
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("ListState", Types.INT);
        descriptor.enableTimeToLive(stateTtlConfig);
        this.listState = context.getOperatorStateStore().getListState(descriptor);
    }
}