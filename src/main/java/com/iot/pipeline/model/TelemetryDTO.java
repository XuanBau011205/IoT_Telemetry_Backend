// TelemetryDTO.java
package com.iot.pipeline.model;

public record TelemetryDTO(
    long    deviceTimestamp, // uint32 epoch — widened to long via & 0xFFFFFFFFL
    float   temperature,
    float   humidity,
    int     nodeId,          // uint8 — widened via Byte.toUnsignedInt()
    int     flags,           // uint8 — widened via Byte.toUnsignedInt()
    int     sequence,        // uint16 — widened via & 0xFFFF
    long    receivedAt       // System.currentTimeMillis() at decode time
) {}