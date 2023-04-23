package com.loda.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author loda
 * @Date 2023/4/22 17:33
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */

//这里要注意：要使用注解指明返回数据的类型
//注解中数据类型的名字（这里的word和length）就是默认sql输出的名字，可以在sql中使用as改名
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class MyUDFTableFunction extends TableFunction<Row> {
    /**
     * 剧情:8_传记:7_动画:3
     * @param types
     */
    public void eval(String types) {
        for (String type : types.split("_")) {
            // use collect(...) to emit a row
            collect(Row.of(type.split(":")[0], Integer.parseInt(type.split(":")[1])));
        }
    }
}
