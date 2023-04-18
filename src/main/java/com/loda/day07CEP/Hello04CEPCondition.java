package com.loda.day07CEP;

import com.loda.pojo.Emp;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * @Author loda
 * @Date 2023/4/18 20:42
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello04CEPCondition {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);
        //source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");

        //transformations
        SingleOutputStreamOperator<Emp> streamOperator = source.map(value -> new ObjectMapper().readValue(value, Emp.class));

        //pattern
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(10);
                    }
                }).times(2).next("middle")
                .where(new IterativeCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value, Context<Emp> ctx) throws Exception {
                        int sum = 0;
                        int count = 0;
                        for (Emp e : ctx.getEventsForPattern("start")) {
//                            System.out.println("sal " + e.getSal());
                            sum += e.getSal();
                            count++;
                        }
                        double avg = sum * 1.0 /count;
                        return avg > 2000;
                    }
                });

        //match
        PatternStream<Emp> patternStream = CEP.pattern(streamOperator, pattern).inProcessingTime();

        //sink
        patternStream.process(new PatternProcessFunction<Emp, Emp>() {
            @Override
            public void processMatch(Map<String, List<Emp>> match, PatternProcessFunction.Context ctx, Collector<Emp> out) throws Exception {
                System.out.println("Hello04CEPCondition.processMatch [ " + match + " ]");
            }
        });

        //env exec
        environment.execute();
    }
}
