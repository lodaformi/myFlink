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
 * @Date 2023/4/18 20:56
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello06CEPPatternStream {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");

        //transformations
        SingleOutputStreamOperator<Emp> streamOperator = source.map(value -> new ObjectMapper().readValue(value, Emp.class));

        //pattern
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getSal() > 1000;
            }
        }).times(2).next("middle").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(20);
            }
        });


        //match
        PatternStream<Emp> patternStream = CEP.pattern(streamOperator, pattern).inProcessingTime();

        //sink
        patternStream.process(new PatternProcessFunction<Emp, String>() {
                    @Override
                    public void processMatch(Map<String, List<Emp>> match, Context ctx, Collector<String> out) throws Exception {
//                        System.out.println("Hello04CEPCondition.processMatch [ " + match + " ]");
                        StringBuffer stringBuffer = new StringBuffer();
                        for (String key : match.keySet()) {
                            stringBuffer.append("[ "+key+" ]{");
                            for (Emp emp : match.get(key)) {
                                stringBuffer.append(emp.getDeptno()+", ");
                            }
                            stringBuffer.append("}");
                        }
                        out.collect(stringBuffer.toString());
                    }
                })
                .print();

        //env exec
        environment.execute();
    }
}
