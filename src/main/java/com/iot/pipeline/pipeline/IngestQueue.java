// IngestQueue.java
package com.iot.pipeline.pipeline;

import com.iot.pipeline.model.EnrichedTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class IngestQueue {

    private static final Logger log = LoggerFactory.getLogger(IngestQueue.class);

    // Bounded queue — acts as the backpressure boundary between the MQTT I/O thread
    // and the database writer thread.  Size tuned for ~200ms flush cycles at ~500 msg/s.
    private static final int CAPACITY = 10_000;

    private final LinkedBlockingQueue<EnrichedTelemetry> queue =
        new LinkedBlockingQueue<>(CAPACITY);

    /**
     * Non-blocking offer — called on the MQTT callback thread.
     * If full, the message is DROPPED (lossy backpressure).  We prefer dropping
     * a single message over blocking the MQTT I/O thread and causing broker disconnects.
     */
    public void offer(EnrichedTelemetry telemetry) {
        boolean accepted = queue.offer(telemetry); // returns immediately — never blocks
        if (!accepted) {
            // STRICT WARNING — this must be visible in ops dashboards / log aggregators.
            log.warn("BACKPRESSURE DROP: queue at capacity [{}]. nodeId={} seq={}",
                CAPACITY,
                telemetry.raw().nodeId(),
                telemetry.raw().sequenceNumber());
        }
    }

    /**
     * Drain up to maxElements into the provided list.
     * Returns the number of elements transferred.
     */
    public int drainTo(List<EnrichedTelemetry> sink, int maxElements) {
        return queue.drainTo(sink, maxElements);
    }

    public int size() {
        return queue.size();
    }

    // Exposed for @PreDestroy flush in DatabaseWriter
    LinkedBlockingQueue<EnrichedTelemetry> rawQueue() {
        return queue;
    }
}