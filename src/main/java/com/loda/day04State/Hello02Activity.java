package com.loda.day04State;

import com.loda.pojo.ActRes;
import com.loda.pojo.Activity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * @Author loda
 * @Date 2023/4/14 20:41
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello02Activity {
    public static void main(String[] args) throws Exception {
        //配置程序执行的参数，指定从哪个checkpoint恢复
        //如果是本地，就找本地的路径，如果是hdfs路径，就要指定hdfs上的路径
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "D:\\Develop\\bigdata\\myFlink01\\ckpt\\f4257fb8d3b1324770d5e885191f5a3f\\chk-9");

        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(5000);
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //获取数据源
        //actId:100,userId:200,amount:3
        //100,200,3
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9998);
        //转换并输出
        source.process(new ProcessFunction<String, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, Tuple3<Integer, Integer, Integer>>.Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(Tuple3.of(Integer.valueOf(split[0]),
                                Integer.valueOf(split[1]), Integer.valueOf(split[2])));
                    }

//                    @Override
//            public void processElement(String value, ProcessFunction<String, Activity>.Context ctx, Collector<Activity> out) throws Exception {
////                value.split()
//                Activity activity = JSON.parseObject(value, Activity.class);
//                out.collect(activity);
//            }
                }).map(new ActOperatorStateFunction2(), TypeInformation.of(ActRes.class))
//                .print().setParallelism(1);
                //为什么连不上？
                .addSink(JdbcSink.sink(
                        "insert into loda.gameActivity(id,actId,person,amount) values (?,?,?,?)",
                        (ps, t) -> {
                            ps.setInt(1, 123);
                            ps.setInt(2, t.getActId());
                            ps.setInt(3, t.getUserCnt());
                            ps.setInt(4, t.getAmountCnt());
                        },
                        JdbcExecutionOptions
                                .builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(3000)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://node01:8123/loda")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUsername("default")
                                .withPassword("123456")
                                .build()));
//                .setParallelism(1);
        //运行环境
        environment.execute();
    }
}

class ActOperatorStateFunction2 implements MapFunction<Tuple3<Integer, Integer, Integer>, ActRes>, CheckpointedFunction {
    //声明一个变量记数
    private int personCount;

    private int amountCount;

    //创建一个状态对象
    private ListState<Integer> personCountListState;

    //创建一个状态对象
    private ListState<Integer> amountCountListState;

    @Override
    public ActRes map(Tuple3<Integer, Integer, Integer> value) throws Exception {
        //更新计数器
        personCount++;
        amountCount = amountCount + value.f2;

        return new ActRes(value.f0, personCount, amountCount);
//        return "[" + value + "][" + personCount + "][" + amountCount + "]";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清除一下历史数据
        //如果不清除，internalList一直会往里添加，比如[2, 4, 4]，[2, 4, 4, 5]
        personCountListState.clear();
        amountCountListState.clear();
        //保存数据
        personCountListState.add(personCount);
        amountCountListState.add(amountCount);

//        System.out.println("YjxxtOperatorStateFunction.snapshotState[" + personCountListState + "][" + amountCountListState  + "][" + System.currentTimeMillis() + "]");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //创建对象的描述器
        ListStateDescriptor<Integer> personDescriptor = new ListStateDescriptor<Integer>("personCountListState", Types.INT);
        ListStateDescriptor<Integer> amountDescriptor = new ListStateDescriptor<Integer>("amountCountListState", Types.INT);
        //创建对象
        this.personCountListState = context.getOperatorStateStore().getListState(personDescriptor);
        this.amountCountListState = context.getOperatorStateStore().getListState(amountDescriptor);
//        if (context.isRestored()) {
//            //恢复成功，将ckpt中的值赋给count
//            for (Integer integer : countListState.get()) {
//                count = integer;
//            }
//            System.out.println("恢复成功");
//        }else {
//            System.out.println("恢复失败");
//        }
    }


    class ActOperatorStateFunction implements MapFunction<Activity, ActRes>, CheckpointedFunction {
        //声明一个变量记数
        private int personCount;

        private int amountCount;

        //创建一个状态对象
        private ListState<Integer> personCountListState;

        //创建一个状态对象
        private ListState<Integer> amountCountListState;

        @Override
        public ActRes map(Activity value) throws Exception {
            //更新计数器
            personCount++;
            amountCount = amountCount + value.getAmount();

            return new ActRes(value.getActId(), personCount, amountCount);
//        return "[" + value + "][" + personCount + "][" + amountCount + "]";
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //清除一下历史数据
            //如果不清除，internalList一直会往里添加，比如[2, 4, 4]，[2, 4, 4, 5]
            personCountListState.clear();
            amountCountListState.clear();
            //保存数据
            personCountListState.add(personCount);
            amountCountListState.add(amountCount);
//        System.out.println("YjxxtOperatorStateFunction.snapshotState[" + personCountListState + "][" + amountCountListState  + "][" + System.currentTimeMillis() + "]");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //创建对象的描述器
            ListStateDescriptor<Integer> personDescriptor = new ListStateDescriptor<Integer>("personCountListState", Types.INT);
            ListStateDescriptor<Integer> amountDescriptor = new ListStateDescriptor<Integer>("amountCountListState", Types.INT);
            //创建对象
            this.personCountListState = context.getOperatorStateStore().getListState(personDescriptor);
            this.amountCountListState = context.getOperatorStateStore().getListState(amountDescriptor);
//        if (context.isRestored()) {
//            //恢复成功，将ckpt中的值赋给count
//            for (Integer integer : countListState.get()) {
//                count = integer;
//            }
//            System.out.println("恢复成功");
//        }else {
//            System.out.println("恢复失败");
//        }
        }

    }
}