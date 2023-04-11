package com.loda.day02EnvSourceTransSink;

import com.loda.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.util.List;

/**
 * @Author loda
 * @Date 2023/4/10 17:05
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello04SourceCustom {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //source
        DataStreamSource<String> source = environment.addSource(new MyCustomSource("data/secret.txt"))
            //并行度大于1，数据会被读多次，在rich中解决
                .setParallelism(2);

        //transformation
        source.print()
                .setParallelism(1);

        //sink

        //env exec
        environment.execute();
    }

}

class MyCustomSource implements ParallelSourceFunction<String> {
    private File file;

    public MyCustomSource(String filePath) {
        this.file = new File(filePath);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        //读取数据文件
        List<String> lines = FileUtils.readLines(file, "utf-8");
        //遍历数据
        for (String line : lines) {
            ctx.collect(DESUtil.decrypt("yjxxt0523", line));
        }
    }

    @Override
    public void cancel() {

    }
}