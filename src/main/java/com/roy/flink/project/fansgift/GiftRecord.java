package com.roy.flink.project.fansgift;

/**
 * @author roy
 * @date 2021/9/15
 * @desc kafka中传入的数据类型
 */
public class GiftRecord {

    private String hostId; //主播ID
    private String fansId; //粉丝ID
    private int giftCount; //礼物数量
    private long giftTime; //送礼物时间。原始时间格式 yyyy-MM-DD HH:mm:ss,sss

    public GiftRecord() {
    }

    public GiftRecord(String hostId, String fansId, int giftCount, long giftTime) {
        this.hostId = hostId;
        this.fansId = fansId;
        this.giftCount = giftCount;
        this.giftTime = giftTime;
    }

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public String getFansId() {
        return fansId;
    }

    public void setFansId(String fansId) {
        this.fansId = fansId;
    }

    public int getGiftCount() {
        return giftCount;
    }

    public void setGiftCount(int giftCount) {
        this.giftCount = giftCount;
    }

    public long getGiftTime() {
        return giftTime;
    }

    public void setGiftTime(long giftTime) {
        this.giftTime = giftTime;
    }

    @Override
    public String toString() {
        return "GiftRecord{" +
                "hostId='" + hostId + '\'' +
                ", fansId='" + fansId + '\'' +
                ", giftCount=" + giftCount +
                ", giftTime='" + giftTime + '\'' +
                '}';
    }
}
