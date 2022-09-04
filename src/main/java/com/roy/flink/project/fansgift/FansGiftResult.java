package com.roy.flink.project.fansgift;

/**
 * @author roy
 * @date 2021/9/15
 * @desc
 */
public class FansGiftResult {

    private String hostId;
    private String fansId;
    private long giftCount;
    private long windowEnd;

    public FansGiftResult() {
    }

    public FansGiftResult(String hostId, String fansId, long giftCount, long windowEnd) {
        this.hostId = hostId;
        this.fansId = fansId;
        this.giftCount = giftCount;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "FansGiftResult{" +
                "hostId='" + hostId + '\'' +
                ", fansId='" + fansId + '\'' +
                ", giftCount=" + giftCount +
                ", windowEnd=" + windowEnd +
                '}';
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

    public long getGiftCount() {
        return giftCount;
    }

    public void setGiftCount(long giftCount) {
        this.giftCount = giftCount;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }
}
