package com.ddsr.watermark;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author ddsr, created it at 2024/2/28 21:25
 */
public class SamplePojo {

    public SamplePojo() {
    }

    private int app;

    @JsonProperty("city_id")
    private int cityId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("topic_type")
    private String topicType;

    private long ts;

    public int getApp() {
        return app;
    }

    public void setApp(int app) {
        this.app = app;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getTopicType() {
        return topicType;
    }

    public void setTopicType(String topicType) {
        this.topicType = topicType;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "SamplePojo{" +
                "ts=" + ts +
                ", topicType='" + topicType + '\'' +
                ", app=" + app +
                ", cityId=" + cityId +
                ", userId='" + userId + '\'' +
                '}';
    }
}
