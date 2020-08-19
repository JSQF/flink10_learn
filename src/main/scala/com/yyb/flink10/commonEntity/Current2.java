package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-03
 * @Time 09:25
 */
public class Current2 {
    private String rowtime;
    private int amount;
    private String currency;
    private Long eventTime;
    public Current2(){

    }

    public Current2(String rowtime, int amount, String currency, Long eventTime) {
        this.rowtime = rowtime;
        this.amount = amount;
        this.currency = currency;
        this.eventTime = eventTime;
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

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "Current2{" +
                "rowtime='" + rowtime + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
