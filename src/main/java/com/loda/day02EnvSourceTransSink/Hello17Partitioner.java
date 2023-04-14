package com.loda.day02EnvSourceTransSink;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author loda
 * @Date 2023/4/11 16:23
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello17Partitioner {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //source
        DataStreamSource<String> source = environment.readTextFile("data/partition.txt").setParallelism(1);

        //transformations
        SingleOutputStreamOperator<String> mapUpper = source.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "task[ " + (getRuntimeContext().getIndexOfThisSubtask() + 1) + " ], value [ " + value + " ]";
            }
        }).setParallelism(2);

        //sink
//        mapUpper.global().print("globalPartitioner ").setParallelism(4);
//        mapUpper.rebalance().print("rebalancePartitioner ").setParallelism(3);
//        mapUpper.rescale().print("rescalePartitioner ").setParallelism(4);
//        mapUpper.shuffle().print("shufflePartitioner ").setParallelism(4);
//        mapUpper.broadcast().print("broadcastPartitioner ").setParallelism(2);
//        mapUpper.forward().print("broadcastPartitioner ").setParallelism(2);
//          mapUpper.keyBy(word->word).print("keyByPartitioner ").setParallelism(4);
        //custom
          mapUpper.partitionCustom(new Partitioner<String>() {
              @Override
              //对那个分区操作，这里是将所有数据写入到最后一个分区中
              public int partition(String key, int numPartitions) {
                  return numPartitions - 1;
              }
          }, new KeySelector<String, String>() {
              @Override
              //对其中的数据做什么处理
              public String getKey(String value) throws Exception {
                  return value;
              }
          }).print("CustomPartitioner ").setParallelism(4);

          //怎么对应上面的传参
          mapUpper.partitionCustom((k, p) -> p-1, v->v).print("CustomPartitioner ").setParallelism(4);

        //env exec
        environment.execute();
    }
}
