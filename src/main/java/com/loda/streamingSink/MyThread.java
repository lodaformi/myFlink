package com.loda.streamingSink;

import com.loda.util.KafkaUtil;

/**
 * @Author loda
 * @Date 2023/7/7 10:49
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class MyThread {
    public static void main(String[] args) {
        new Thread(() -> {
            for (int i = 5000; i < 15000; i++) {
                KafkaUtil.sendMsg("kfkSource", "Hello Flink" + i + "--" + System.currentTimeMillis());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
