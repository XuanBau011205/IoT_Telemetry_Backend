// EnrichedTelemetry.java  — internal transfer object after windowing
package com.iot.pipeline.model;

public record EnrichedTelemetry(
    TelemetryDTO raw,
    float windowAvgTemperature
) {}