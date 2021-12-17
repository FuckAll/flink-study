package org.apache.flink.bean;

/**
 * @author izgnod
 */
public class SensorReading {
    public SensorReading(String id, Long temperature, Long eventTime) {
        this.id = id;
        this.temperature = temperature;
        this.eventTime = eventTime;
    }

    private String id;
    private Long temperature;
    private Long eventTime;

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTemperature() {
        return temperature;
    }

    public void setTemperature(Long temperature) {
        this.temperature = temperature;
    }
}
