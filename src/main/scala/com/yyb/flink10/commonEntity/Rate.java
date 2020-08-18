package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-06
 * @Time 17:56
 */
public class Rate {
    private String rowtime;
    private String currency;
    private int rate;

    public Rate() {
    }

    public Rate(String rowtime, String currency, Integer rate) {
        this.rowtime = rowtime;
        this.currency = currency;
        this.rate = rate;
    }

    public String getRowtime() {
        return rowtime;
    }

    public void setRowtime(String rowtime) {
        this.rowtime = rowtime;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    @Override
    public String toString() {
        return "Rate{" +
                "rowtime='" + rowtime + '\'' +
                ", currency='" + currency + '\'' +
                ", rate=" + rate +
                '}';
    }
}
