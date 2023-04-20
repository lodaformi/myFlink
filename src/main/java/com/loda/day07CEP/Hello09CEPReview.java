package com.loda.day07CEP;

import com.loda.pojo.Emp;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
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
 * @Date 2023/4/20 20:39
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello09CEPReview {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");

        //transformation
        SingleOutputStreamOperator<Emp> empDS = source.map(value -> new ObjectMapper().readValue(value, Emp.class));
        //pattern
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(20);
            }
        });

        //datastream + pattern
        PatternStream<Emp> empPatternStream = CEP.pattern(empDS, pattern).inProcessingTime();

        //sink
        SingleOutputStreamOperator<String> process = empPatternStream.process(new PatternProcessFunction<Emp, String>() {
            @Override
            public void processMatch(Map<String, List<Emp>> match, Context ctx, Collector<String> out) throws Exception {

                System.out.println("processMatch ["+ match + "]");
//                for (String s : match.keySet()) {
//                    System.out.println("Hello09CEPReview.processMatch [" + s + "--" + match.get(s) + "]");
//                }
            }
        });

        //env exec
        environment.execute();
    }
}
