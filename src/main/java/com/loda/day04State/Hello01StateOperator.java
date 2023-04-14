package com.loda.day04State;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @Author loda
 * @Date 2023/4/14 20:40
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01StateOperator {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.enableCheckpointing(5000);
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //获取数据源
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //转换并输出
//         source.map(word -> word.toUpperCase()).print();
        //转换需要添加当前SubTask处理这个单词的序号并输出
        source.map(new YjxxtOperatorStateFunction()).print();
        //运行环境
        environment.execute();
    }
}

class YjxxtOperatorStateFunction implements MapFunction<String, String>, CheckpointedFunction {

    //声明一个变量记数
    private int count;
    //创建一个状态对象
    private ListState<Integer> countListState;

    @Override
    public String map(String value) throws Exception {
        //更新计数器
        count++;
        return "[" + value.toUpperCase() + "][" + count + "]";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清除一下历史数据
        countListState.clear();
        //保存数据
        countListState.add(count);
        System.out.println("YjxxtOperatorStateFunction.snapshotState[" + countListState + "][" + System.currentTimeMillis() + "]");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //创建对象的描述器
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>("CountListState", Types.INT);
        //创建对象
        this.countListState = context.getOperatorStateStore().getListState(descriptor);
    }
}
