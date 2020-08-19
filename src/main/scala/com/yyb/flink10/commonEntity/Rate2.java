package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-06
 * @Time 17:56
 */
public class Rate2 {
    private String rowtime;
    private String currency;
    private int rate;
    private Long eventTime;

    public Rate2() {
    }

    public Rate2(String rowtime, String currency, int rate, Long eventTime) {
        this.rowtime = rowtime;
        this.currency = currency;
        this.rate = rate;
        this.eventTime = eventTime;
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

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "Rate2{" +
                "rowtime='" + rowtime + '\'' +
                ", currency='" + currency + '\'' +
                ", rate=" + rate +
                ", eventTime=" + eventTime +
                '}';
    }
}
