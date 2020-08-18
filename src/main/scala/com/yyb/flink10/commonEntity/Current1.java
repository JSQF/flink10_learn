package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-03
 * @Time 09:25
 */
public class Current1 {
    private String rowtime;
    private int amount;
    private String currency;
    public Current1(){

    }

    public Current1(String rowtime, int amount, String currency) {
        this.rowtime = rowtime;
        this.amount = amount;
        this.currency = currency;
    }

    public String getRowtime() {
        return rowtime;
    }

    public void setRowtime(String rowtime) {
        this.rowtime = rowtime;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    @Override
    public String toString() {
        return "Current1{" +
                "rowtime='" + rowtime + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                '}';
    }
}
