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
 * @Date 2023/4/18 17:32
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello03CEPPattern02 {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

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
        })
        //上一个条件匹配成功的数据，next紧挨着的数据要符合下面的条件，如果有这样的数据，则表示匹配到。
        .next("deptno20").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp value) throws Exception {
                return value.getDeptno().equals(20);
            }
        });

        Pattern<Emp, Emp> pattern2 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(10);
                    }
                })
                .followedBy("deptno20").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(20);
                    }
                });

        Pattern<Emp, Emp> pattern3 = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(10);
                    }
                })
                .followedByAny("deptno20").where(new SimpleCondition<Emp>() {
                    @Override
                    public boolean filter(Emp value) throws Exception {
                        return value.getDeptno().equals(20);
                    }
                });


        //match
        PatternStream<Emp> patternStream = CEP.pattern(streamOperator, pattern).inProcessingTime();

        //sink
        patternStream.process(new PatternProcessFunction<Emp, Emp>() {
            @Override
            public void processMatch(Map<String, List<Emp>> match, Context ctx, Collector<Emp> out) throws Exception {
                System.out.println("Hello03CEPPattern02.processMatch [ " + match + " ]");
            }
        });

        //env exec
        environment.execute();
    }
}
