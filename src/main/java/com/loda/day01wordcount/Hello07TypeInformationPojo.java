package com.loda.day01wordcount;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author loda
 * @Date 2023/4/8 22:11
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello07TypeInformationPojo {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> streamSource = environment.socketTextStream("localhost", 9999);

        //transformations+sink
        streamSource.map(line ->
            new User((int)(Math.random()*9000+1000), line.split("-")[0], line.split("-")[1])
        ).print();
        //env exec
        environment.execute();
    }
}

class User implements Serializable {
    private Integer id;
    private String uname;
    private String passwd;

    public User() {
    }

    public User(Integer id, String uname, String passwd) {
        this.id = id;
        this.uname = uname;
        this.passwd = passwd;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(id, user.id) && Objects.equals(uname, user.uname) && Objects.equals(passwd, user.passwd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, uname, passwd);
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", uname='" + uname + '\'' +
                ", passwd='" + passwd + '\'' +
                '}';
    }
}