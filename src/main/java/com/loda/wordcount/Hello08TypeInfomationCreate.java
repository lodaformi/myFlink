package com.loda.wordcount;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author loda
 * @Date 2023/4/8 22:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello08TypeInfomationCreate {
    public static void main(String[] args) {
        //method1
        TypeInformation<String> of = TypeInformation.of(String.class);

        //method2
        TypeInformation<Tuple2<String, Long>> info = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        });

        //method3
        TypeInformation<String> string = Types.STRING;
        TypeInformation<Tuple> tuple = Types.TUPLE(Types.STRING, Types.LONG);
    }
}
