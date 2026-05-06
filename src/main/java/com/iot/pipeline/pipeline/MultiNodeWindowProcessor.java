// MultiNodeWindowProcessor.java
package com.iot.pipeline.pipeline;

import com.iot.pipeline.model.EnrichedTelemetry;
import com.iot.pipeline.model.TelemetryDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MultiNodeWindowProcessor {

    private static final Logger log = LoggerFactory.getLogger(MultiNodeWindowProcessor.class);

    // TTL: 5 minutes in milliseconds — nodes that stop sending are evicted.
    private static final long WINDOW_TTL_MS = 5 * 60 * 1000L;

    // ConcurrentHashMap for lock-striped concurrent access across many nodeIds.
    // Each key is a nodeId; contention is per-node, not global.
    private final ConcurrentHashMap<Integer, WindowState> windows = new ConcurrentHashMap<>();

    /**
     * Apply the sliding window for the given node and return enriched telemetry.
     *
     * ConcurrentHashMap.compute() is atomic per-key: it holds the stripe lock
     * for nodeId's bucket while the lambda executes, giving us safe read-modify-write
     * without a separate synchronized block.
     */
    public EnrichedTelemetry process(TelemetryDTO dto) {
        // compute() is atomic for the given key — no external lock needed.
        float[] avgHolder = new float[1]; // capture from lambda

        windows.compute(dto.nodeId(), (id, state) -> {
            if (state == null) state = new WindowState();
            avgHolder[0] = state.addAndAverage(dto.temperature());
            return state;
        });

        return new EnrichedTelemetry(dto, avgHolder[0]);
    }

    /**
     * Background cleanup — evicts WindowState entries for nodes that have been
     * silent for longer than WINDOW_TTL_MS.  Runs every 60 seconds.
     *
     * ConcurrentHashMap.entrySet().iterator() is weakly-consistent: it is safe
     * to call remove() via the iterator without ConcurrentModificationException.
     */
    @Scheduled(fixedDelay = 60_000)
    public void evictStaleWindows() {
        long now = System.currentTimeMillis();
        int evicted = 0;

        Iterator<Map.Entry<Integer, WindowState>> it = windows.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, WindowState> entry = it.next();
            long idle = now - entry.getValue().lastUpdated.get();
            if (idle > WINDOW_TTL_MS) {
                it.remove();
                evicted++;
            }
        }

        if (evicted > 0) {
            log.info("TTL eviction: removed {} stale window(s). Active windows: {}", evicted, windows.size());
        }
    }
}