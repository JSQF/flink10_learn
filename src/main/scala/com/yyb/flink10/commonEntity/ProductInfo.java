package com.yyb.flink10.commonEntity;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-08-19
 * @Time 09:17
 */
public class ProductInfo {
    private String productID;
    private String productName;
    private String productCategory;
    private String updatedAt;
    private Long updatedAtTimestamp;

    public ProductInfo() {
    }

    public ProductInfo(String productID, String productName, String productCategory, String updatedAt, Long updatedAtTimestamp) {
        this.productID = productID;
        this.productName = productName;
        this.productCategory = productCategory;
        this.updatedAt = updatedAt;
        this.updatedAtTimestamp = updatedAtTimestamp;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getUpdatedAtTimestamp() {
        return updatedAtTimestamp;
    }

    public void setUpdatedAtTimestamp(Long updatedAtTimestamp) {
        this.updatedAtTimestamp = updatedAtTimestamp;
    }

    @Override
    public String toString() {
        return "ProductInfo{" +
                "productID='" + productID + '\'' +
                ", productName='" + productName + '\'' +
                ", productCategory='" + productCategory + '\'' +
                ", updatedAt='" + updatedAt + '\'' +
                ", updatedAtTimestamp=" + updatedAtTimestamp +
                '}';
    }
}
