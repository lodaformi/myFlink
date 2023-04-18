package com.loda.day07CEP;

import com.loda.pojo.Emp;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * @Author loda
 * @Date 2023/4/18 21:17
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08CEPSkip {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //Source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");
        DataStream<Emp> stream = source.map(line -> new ObjectMapper().readValue(line, Emp.class));
        //声明模式
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start", AfterMatchSkipStrategy.skipToNext()).where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp emp) throws Exception {
                return emp.getSal() > 1000;
            }
        }).times(1, 3);

        //开始匹配流和模式
        PatternStream<Emp> patternStream = CEP.pattern(stream, pattern).inProcessingTime();

        //开始处理匹配的结果
        patternStream.process(new PatternProcessFunction<Emp, Emp>() {
            @Override
            public void processMatch(Map<String, List<Emp>> map, Context context, Collector<Emp> collector) throws Exception {
                System.out.println("Hello02CEPByExample.processMatch[" + map + "]");
            }
        });

        //运行环境
        environment.execute();

    }
}
