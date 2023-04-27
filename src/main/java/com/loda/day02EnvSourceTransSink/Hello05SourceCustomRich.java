package com.loda.day02EnvSourceTransSink;

import com.loda.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/10 17:22
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello05SourceCustomRich {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //source
        DataStreamSource<String> source = environment.addSource(new myCustomSourceRich("data/secret.txt")).setParallelism(3);
        //DataStreamSource<String> source = environment.fromElements("aa","bb");

//        source.rebalance().map(value -> value.toUpperCase()).print("source--").setParallelism(1);

        //transformations
        source.rebalance().map(new RichMapFunction<String, String>() {
            @Override
            //代码运行的时候自动设置的上下文环境
            public void setRuntimeContext(RuntimeContext t) {
//                 System.out.println("Hello05SourceCustomRich.setRuntimeContext " + System.currentTimeMillis());
                super.setRuntimeContext(t);
            }

            @Override
            //为了获取系统的上下文环境
            public RuntimeContext getRuntimeContext() {
//                 System.out.println("Hello05SourceCustomRich.getRuntimeContext " + System.currentTimeMillis());
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
//                 System.out.println("Hello05SourceCustomRich.getIterationRuntimeContext " + System.currentTimeMillis());
                return super.getIterationRuntimeContext();
            }

            @Override
            //相当于Junit的before注解，程序开始前要做的操作
            public void open(Configuration parameters) throws Exception {
//                 System.out.println("Hello05SourceCustomRich.open " + System.currentTimeMillis());
                super.open(parameters);
            }

            @Override
            //相当于Junit的after注解，程序结束时要做的操作
            public void close() throws Exception {
//                 System.out.println("Hello05SourceCustomRich.close " + System.currentTimeMillis());
                super.close();
            }

            @Override
            //业务逻辑部分
            public String map(String value) throws Exception {
                return value.toUpperCase() + ":" + getRuntimeContext().getIndexOfThisSubtask();
            }
        }).setParallelism(2).print();
        
        //sink

        //env exec
        environment.execute();
    }
}

class myCustomSourceRich extends RichParallelSourceFunction<String> {
    private String filePath;
    //总并行数
    private int numberOfParallelSubtasks;
    //每个task的编号index
    private int indexOfThisSubtask;

    public myCustomSourceRich(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("myCustomSourceRich的并行度是："+this.numberOfParallelSubtasks+"，当前task的编号为："+this.indexOfThisSubtask);
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath), "utf-8");
        //遍历数据
        for (String line : lines) {
            if (Math.abs(line.hashCode()) % numberOfParallelSubtasks == indexOfThisSubtask) {
                ctx.collect(DESUtil.decrypt("yjxxt0523", line) + "[index#" + this.indexOfThisSubtask + "]");
            }
        }
    }

    @Override
    public void cancel() {
    }
}