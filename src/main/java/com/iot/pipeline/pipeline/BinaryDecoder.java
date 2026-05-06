// BinaryDecoder.java
package com.iot.pipeline.pipeline;

import com.iot.telemetry.model.TelemetryDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Stateless, zero-allocation decoder for the ESP32 OcsPayload struct.
 *
 * Wire layout (little-endian, exactly 16 bytes):
 * ┌──────────┬──────┬──────────────────────────────────────────────────────┐
 * │ Offset   │ Size │ Field                                                │
 * ├──────────┼──────┼──────────────────────────────────────────────────────┤
 * │  0 – 3   │  4   │ timestamp_sec  (uint32 → long via & 0xFFFFFFFFL)     │
 * │  4 – 7   │  4   │ temperature    (IEEE-754 float, LE)                  │
 * │  8 – 11  │  4   │ humidity       (IEEE-754 float, LE)                  │
 * │ 12       │  1   │ node_id        (uint8  → int  via toUnsignedInt)     │
 * │ 13       │  1   │ flags          (uint8  → int  via toUnsignedInt)     │
 * │ 14 – 15  │  2   │ sequence       (uint16 → int  via & 0xFFFF)          │
 * └──────────┴──────┴──────────────────────────────────────────────────────┘
 */
@Component
public class BinaryDecoder {

    private static final Logger log            = LoggerFactory.getLogger(BinaryDecoder.class);
    private static final int    EXPECTED_BYTES = 16;

    /**
     * Decodes a raw MQTT payload into a {@link TelemetryDTO}.
     *
     * @return {@code Optional.empty()} if the payload is null or not exactly 16 bytes.
     *         The caller decides whether to log, count, or silently drop.
     */
    public Optional<TelemetryDTO> decode(byte[] payload) {

        // ── Length guard ─────────────────────────────────────────────────────
        if (payload == null || payload.length != EXPECTED_BYTES) {
            log.warn("Dropping frame: expected {} bytes, got {}",
                EXPECTED_BYTES, payload == null ? "null" : payload.length);
            return Optional.empty();
        }

        // ByteBuffer.wrap() does NOT copy the array — zero allocation over the
        // existing byte[].  order(LITTLE_ENDIAN) matches the ESP32's native byte order.
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);

        // ── timestamp_sec (uint32) ────────────────────────────────────────────
        // buf.getInt() returns a signed Java int32.
        // Masking with 0xFFFFFFFFL promotes to long and zeroes the sign-extension
        // bits, correctly representing the full unsigned [0, 4_294_967_295] range.
        long deviceTimestamp = buf.getInt(0) & 0xFFFFFFFFL;

        // ── temperature & humidity (IEEE-754 float, little-endian) ────────────
        // getFloat() reads 4 bytes and interprets them as IEEE-754 — no conversion needed.
        float temperature = buf.getFloat(4);
        float humidity    = buf.getFloat(8);

        // ── node_id (uint8) ───────────────────────────────────────────────────
        // buf.get(12) returns a signed byte [-128, 127].
        // Byte.toUnsignedInt() is equivalent to (b & 0xFF); it widens to int [0, 255].
        int nodeId = Byte.toUnsignedInt(buf.get(12));

        // ── flags (uint8) — same unsigned promotion ───────────────────────────
        int flags = Byte.toUnsignedInt(buf.get(13));

        // ── sequence (uint16) ─────────────────────────────────────────────────
        // buf.getShort(14) returns a signed int16 [-32768, 32767].
        // Masking with 0xFFFF promotes to int and clears the sign bit,
        // giving the correct unsigned range [0, 65535].
        int sequence = buf.getShort(14) & 0xFFFF;

        return Optional.of(new TelemetryDTO(
            deviceTimestamp,
            temperature,
            humidity,
            nodeId,
            flags,
            sequence,
            System.currentTimeMillis() // receivedAt — backend wall-clock
        ));
    }
}