package com.roy.flink.beans;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class Stock {
    private String id;
    private Double price;
    private String stockName;
    private long timestamp;

    public Stock() {
    }

    public Stock(String id, Double price, String stockName, long timestamp) {
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

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public void setPrice(Double price) {
        this.price = price;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "id='" + id + '\'' +
                ", price=" + price +
                ", stockName='" + stockName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
