package com.loda.streamingSink;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Author loda
 * @Date 2023/7/7 17:14
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Time2Date {
    public static void main(String[] args) {
        //1688721446737
        long millis = System.currentTimeMillis();
        System.out.println(millis);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd-HH");
        System.out.println(LocalDateTime.now());
        System.out.println(LocalDate.now());
        String s = formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
        System.out.println(s);
        System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        System.out.println(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));

    }
}
