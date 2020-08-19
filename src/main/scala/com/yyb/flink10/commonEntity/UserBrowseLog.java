package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-19
 * @Time 09:05
 */
public class UserBrowseLog {
    private String userID;
    private String eventTime;
    private String eventType;
    private String productID;
    private Integer productPrice;
    private Long eventTimeTimestamp;

    public UserBrowseLog() {
    }

    public UserBrowseLog(String userID, String eventTime, String eventType, String productID, Integer productPrice, Long eventTimeTimestamp) {
        this.userID = userID;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.productID = productID;
        this.productPrice = productPrice;
        this.eventTimeTimestamp = eventTimeTimestamp;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public Integer getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Integer productPrice) {
        this.productPrice = productPrice;
    }

    public Long getEventTimeTimestamp() {
        return eventTimeTimestamp;
    }

    public void setEventTimeTimestamp(Long eventTimeTimestamp) {
        this.eventTimeTimestamp = eventTimeTimestamp;
    }

    @Override
    public String toString() {
        return "UserBrowseLog{" +
                "userID='" + userID + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", productID='" + productID + '\'' +
                ", productPrice=" + productPrice +
                ", eventTimeTimestamp=" + eventTimeTimestamp +
                '}';
    }
}
