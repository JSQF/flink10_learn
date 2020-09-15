package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-06-10
 * @Time 11:38
 */
public class Pi{
    private String id;
    private String time;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Pi(String id, String time) {
        this.id = id;
        this.time = time;
    }
}
