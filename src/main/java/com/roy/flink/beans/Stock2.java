package com.roy.flink.beans;

import java.sql.Timestamp;

/**
 * @author roy
 * @date 2021/9/13
 * @desc
 */
public class Stock2 {
    private String id;
    private Double price;
    private String stockName;
    private Timestamp timestamp;

    public Stock2(String id, Double price, String stockName, Timestamp timestamp) {
        this.id = id;
        this.price = price;
        this.stockName = stockName;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Stock2{" +
                "id='" + id + '\'' +
                ", price=" + price +
                ", stockName='" + stockName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
