// TelemetryEntity.java
package com.iot.telemetry.model;

import jakarta.persistence.*;

@Entity
@Table(
    name = "telemetry",
    indexes = {
        @Index(name = "idx_node_device_ts", columnList = "nodeId, deviceTimestamp"),
        @Index(name = "idx_received_at",    columnList = "receivedAt")
    }
)
public class TelemetryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private int nodeId;

    @Column(nullable = false)
    private float temperature;

    @Column(nullable = false)
    private float humidity;

    @Column(nullable = false)
    private int flags;

    @Column(nullable = false)
    private int sequence;

    /** Unix epoch seconds as reported by the ESP32 RTC. */
    @Column(nullable = false)
    private long deviceTimestamp;

    /** Backend wall-clock millis at the moment this frame was processed. */
    @Column(nullable = false)
    private long receivedAt;

    protected TelemetryEntity() {}

    public TelemetryEntity(int nodeId, float temperature, float humidity,
                           int flags, int sequence,
                           long deviceTimestamp, long receivedAt) {
        this.nodeId          = nodeId;
        this.temperature     = temperature;
        this.humidity        = humidity;
        this.flags           = flags;
        this.sequence        = sequence;
        this.deviceTimestamp = deviceTimestamp;
        this.receivedAt      = receivedAt;
    }

    public Long  getId()              { return id; }
    public int   getNodeId()          { return nodeId; }
    public float getTemperature()     { return temperature; }
    public float getHumidity()        { return humidity; }
    public int   getFlags()           { return flags; }
    public int   getSequence()        { return sequence; }
    public long  getDeviceTimestamp() { return deviceTimestamp; }
    public long  getReceivedAt()      { return receivedAt; }
}