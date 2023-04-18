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
 * @Date 2023/4/18 17:05
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello02CEPPattern01 {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");

        //transformaitons
        SingleOutputStreamOperator<Emp> stream = source.map(line -> new ObjectMapper().readValue(line, Emp.class));

        //pattern
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start")
                .where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(20);
                    }
        }).where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return "CLERK".equals(value.getJob());
                    }
                });

        Pattern<Emp, Emp> pattern1 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(10);
            }
        })
        //精确限制一个输出结果中数据的个数，超过匹配数据条目时，不会输出结果
        .times(5);

        Pattern<Emp, Emp> pattern2 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(10);
            }
        })
        .times(2,5);



        Pattern<Emp, Emp> pattern3 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(10);
            }
        }).where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getSal() > 1500;
            }
        });

        Pattern<Emp, Emp> pattern4 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(10);
            }
        })
            //什么意思？
            .oneOrMore().until(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getSal() > 300;
            }
        });

        Pattern<Emp, Emp> pattern5 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(10);
                    }
                })
                // 1次或多次
                .oneOrMore();

        //match
        PatternStream<Emp> empPatternStream = CEP.pattern(stream, pattern5).inProcessingTime();

        //sink
        empPatternStream.process(new PatternProcessFunction<Emp, Emp>() {
            @Override
            public void processMatch(Map<String, List<Emp>> match, Context ctx, Collector<Emp> out) throws Exception {
                System.out.println("Hello02CEPForPattern.processMatch ["+ match+"]");
            }
        });

        //env exec
        environment.execute();
    }
}
