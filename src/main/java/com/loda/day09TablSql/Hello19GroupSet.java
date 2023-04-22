package com.loda.day09TablSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author loda
 * @Date 2023/4/21 19:56
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello19GroupSet {
    public static void main(String[] args) {
        //env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        // GROUPING SETS
//         tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
//                 "FROM (VALUES\n" +
//                 " ('省1','市1','县1',100),\n" +
//                 " ('省1','市2','县2',101),\n" +
//                 " ('省1','市2','县1',102),\n" +
//                 " ('省2','市1','县4',103),\n" +
//                 " ('省2','市2','县1',104),\n" +
//                 " ('省2','市2','县1',105),\n" +
//                 " ('省3','市1','县1',106),\n" +
//                 " ('省3','市2','县1',107),\n" +
//                 " ('省3','市2','县2',108),\n" +
//                 " ('省4','市1','县1',109),\n" +
//                 " ('省4','市2','县1',110))\n" +
//                 "AS t_person_num(pid, cid, xid, num)\n" +
//                 "GROUP BY GROUPING SETS ((pid, cid, xid),(pid, cid),(pid), ())").execute().print();

         // rollup
//        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
//                "FROM (VALUES\n" +
//                " ('省1','市1','县1',100),\n" +
//                " ('省1','市2','县2',101),\n" +
//                " ('省1','市2','县1',102),\n" +
//                " ('省2','市1','县4',103),\n" +
//                " ('省2','市2','县1',104),\n" +
//                " ('省2','市2','县1',105),\n" +
//                " ('省3','市1','县1',106),\n" +
//                " ('省3','市2','县1',107),\n" +
//                " ('省3','市2','县2',108),\n" +
//                " ('省4','市1','县1',109),\n" +
//                " ('省4','市2','县1',110))\n" +
//                "AS t_person_num(pid, cid, xid, num)\n" +
//                "GROUP BY rollup (pid, cid, xid)").execute().print();
                //设置强制维度pid
                //"GROUP BY pid, rollup (cid, xid)").execute().print();

        //cube
        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid, num)\n" +
                "GROUP BY cube (pid, cid, xid)").execute().print();

    }
}
