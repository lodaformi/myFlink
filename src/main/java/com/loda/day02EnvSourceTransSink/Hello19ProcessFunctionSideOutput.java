package com.loda.day02EnvSourceTransSink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author loda
 * @Date 2023/4/11 17:39
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello19ProcessFunctionSideOutput {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.fromElements("A", "aA", "aaA", "B", "bB", "bBb");

        OutputTag<String> outputTag1 = new OutputTag<>("outputTag1"){};
        OutputTag<String> outputTag2 = new OutputTag<>("outputTag2"){};

        //transformations
        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.length() == 1) {
                    ctx.output(outputTag1, value.toLowerCase());
                } else if (value.length() == 2) {
                    ctx.output(outputTag2, value.toLowerCase());
                }
//                else {
//
//                }
                out.collect(value.toUpperCase());
            }
        });

        //sink
        process.getSideOutput(outputTag1).print("outSide1 ").setParallelism(1);
        process.getSideOutput(outputTag2).print("outSide2 ").setParallelism(1);

        process.print();
        //env exec
        environment.execute();
    }
}
