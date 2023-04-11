package com.loda.day02EnvSourceTransSink;

import com.loda.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/10 23:44
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello15SinkToCustom {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        //操作数据
        ArrayList<String> list = new ArrayList<>();
        list.add("君子周而不比，小人比而不周");
        list.add("君子喻于义，小人喻于利");
        list.add("君子怀德，小人怀土；君子怀刑，小人怀惠");
        list.add("君子欲讷于言而敏于行");
        list.add("君子坦荡荡，小人长戚戚");
        DataStreamSource<String> source = environment.fromCollection(list);

        //无法将数据写入到多个文件中？？
        source.addSink(new MyCustomSink("data/sink" + System.currentTimeMillis())).setParallelism(2);

        //运行环境
        environment.execute();

    }
}

class MyCustomSink implements SinkFunction<String> {
    private File file;

    public MyCustomSink(String filePath) {
        this.file = new File(filePath);
    }
    @Override
    public void invoke(String line, Context context) throws Exception {
        //加密数据
        String encrypt = DESUtil.encrypt("yjxxt0523", line) + "\r\n";
        //写出数据
        FileUtils.writeStringToFile(file, encrypt, "utf-8", true);
    }
}
