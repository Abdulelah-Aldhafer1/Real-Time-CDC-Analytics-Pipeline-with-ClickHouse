package com.cdc.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.time.LocalDateTime;

public class EngagementEvent implements Serializable {
    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("content_id")
    private String contentId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("timestamp")
    private LocalDateTime timestamp;

    @JsonProperty("session_id")
    private String sessionId;

    @JsonProperty("platform")
    private String platform;

    @JsonProperty("duration")
    private Long duration;

    @JsonProperty("engagement_score")
    private Double engagementScore;

    @JsonProperty("content_type")
    private String contentType;

    @JsonProperty("length_seconds")
    private Integer lengthSeconds;

    @JsonProperty("engagement_seconds")
    private Double engagementSeconds;

    @JsonProperty("engagement_pct")
    private Double engagementPct;

    public EngagementEvent() {}

    public EngagementEvent(String eventId, String userId, String contentId, String eventType, 
                          LocalDateTime timestamp, String sessionId, String platform, 
                          Long duration, Double engagementScore) {
        this.eventId = eventId;
        this.userId = userId;
        this.contentId = contentId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.sessionId = sessionId;
        this.platform = platform;
        this.duration = duration;
        this.engagementScore = engagementScore;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getPlatform() { return platform; }
    public void setPlatform(String platform) { this.platform = platform; }

    public Long getDuration() { return duration; }
    public void setDuration(Long duration) { this.duration = duration; }

    public Double getEngagementScore() { return engagementScore; }
    public void setEngagementScore(Double engagementScore) { this.engagementScore = engagementScore; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public Integer getLengthSeconds() { return lengthSeconds; }
    public void setLengthSeconds(Integer lengthSeconds) { this.lengthSeconds = lengthSeconds; }

    public Double getEngagementSeconds() { return engagementSeconds; }
    public void setEngagementSeconds(Double engagementSeconds) { this.engagementSeconds = engagementSeconds; }

    public Double getEngagementPct() { return engagementPct; }
    public void setEngagementPct(Double engagementPct) { this.engagementPct = engagementPct; }

    @Override
    public String toString() {
        return "EngagementEvent{" +
                "eventId='" + eventId + '\'' +
                ", userId='" + userId + '\'' +
                ", contentId='" + contentId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", sessionId='" + sessionId + '\'' +
                ", platform='" + platform + '\'' +
                ", duration=" + duration +
                ", engagementScore=" + engagementScore +
                ", contentType='" + contentType + '\'' +
                ", lengthSeconds=" + lengthSeconds +
                ", engagementSeconds=" + engagementSeconds +
                ", engagementPct=" + engagementPct +
                '}';
    }
}