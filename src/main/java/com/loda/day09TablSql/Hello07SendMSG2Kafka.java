package com.loda.day09TablSql;

import com.loda.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * @Author loda
 * @Date 2023/4/19 17:57
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07SendMSG2Kafka {
    public static void main(String[] args) {
        for (int i = 20; i < 30; i++) {
            KafkaUtil.sendMsg("user_source", i + "," + RandomStringUtils.randomAlphabetic(8));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
