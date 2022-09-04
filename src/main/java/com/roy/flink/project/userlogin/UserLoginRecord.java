package com.roy.flink.project.userlogin;

/**
 * @author roy
 * @date 2021/9/17
 * @desc
 */
public class UserLoginRecord {
    private String userId;
    private int loginRes; // 0-成功， 1-失败
    private long loginTime;

    public UserLoginRecord() {
    }

    public UserLoginRecord(String userId, int loginRes, long loginTime) {
        this.userId = userId;
        this.loginRes = loginRes;
        this.loginTime = loginTime;
    }

    @Override
    public String toString() {
        return "UserLoginRecord{" +
                "userId='" + userId + '\'' +
                ", loginRes=" + loginRes +
                ", loginTime=" + loginTime +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getLoginRes() {
        return loginRes;
    }

    public void setLoginRes(int loginRes) {
        this.loginRes = loginRes;
    }

    public long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(long loginTime) {
        this.loginTime = loginTime;
    }
}
