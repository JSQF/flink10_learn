package com.yyb.flink10.commonEntity;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-09-04
 * @Time 17:12
 */
public class User {
    private java.sql.Timestamp visit_time;
    private long user_id;
    private String user_name;
    private boolean sex;
    private int age;
    private float ext_float;
    private double ext_double;
    private java.math.BigDecimal ext_decimal;
    private java.sql.Date ext_date;
    private java.sql.Time ext_time;
    private short ext_smallint;
    private byte ext_tinyint;

    public User() {

    }

    public User(Timestamp visit_time, long user_id, String user_name, boolean sex, int age, float ext_float, double ext_double, BigDecimal ext_decimal, Date ext_date, Time ext_time, short ext_smallint, byte ext_tinyint) {
        this.visit_time = visit_time;
        this.user_id = user_id;
        this.user_name = user_name;
        this.sex = sex;
        this.age = age;
        this.ext_float = ext_float;
        this.ext_double = ext_double;
        this.ext_decimal = ext_decimal;
        this.ext_date = ext_date;
        this.ext_time = ext_time;
        this.ext_smallint = ext_smallint;
        this.ext_tinyint = ext_tinyint;
    }

    public Timestamp getVisit_time() {
        return visit_time;
    }

    public void setVisit_time(Timestamp visit_time) {
        this.visit_time = visit_time;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public boolean isSex() {
        return sex;
    }

    public void setSex(boolean sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public float getExt_float() {
        return ext_float;
    }

    public void setExt_float(float ext_float) {
        this.ext_float = ext_float;
    }

    public double getExt_double() {
        return ext_double;
    }

    public void setExt_double(double ext_double) {
        this.ext_double = ext_double;
    }

    public BigDecimal getExt_decimal() {
        return ext_decimal;
    }

    public void setExt_decimal(BigDecimal ext_decimal) {
        this.ext_decimal = ext_decimal;
    }

    public Date getExt_date() {
        return ext_date;
    }

    public void setExt_date(Date ext_date) {
        this.ext_date = ext_date;
    }

    public Time getExt_time() {
        return ext_time;
    }

    public void setExt_time(Time ext_time) {
        this.ext_time = ext_time;
    }

    public short getExt_smallint() {
        return ext_smallint;
    }

    public void setExt_smallint(short ext_smallint) {
        this.ext_smallint = ext_smallint;
    }

    public byte getExt_tinyint() {
        return ext_tinyint;
    }

    public void setExt_tinyint(byte ext_tinyint) {
        this.ext_tinyint = ext_tinyint;
    }

    @Override
    public String toString() {
        return "User{" +
                "visit_time=" + visit_time +
                ", user_id=" + user_id +
                ", user_name='" + user_name + '\'' +
                ", sex=" + sex +
                ", age=" + age +
                ", ext_float=" + ext_float +
                ", ext_double=" + ext_double +
                ", ext_decimal=" + ext_decimal +
                ", ext_date=" + ext_date +
                ", ext_time=" + ext_time +
                ", ext_smallint=" + ext_smallint +
                ", ext_tinyint=" + ext_tinyint +
                '}';
    }
}
