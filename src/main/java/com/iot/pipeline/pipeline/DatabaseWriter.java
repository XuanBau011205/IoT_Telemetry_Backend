// DatabaseWriter.java
package com.iot.pipeline.pipeline;

import com.iot.pipeline.model.EnrichedTelemetry;
import com.iot.pipeline.model.TelemetryEntity;
import com.iot.pipeline.repository.TelemetryRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DatabaseWriter {

    private static final Logger log = LoggerFactory.getLogger(DatabaseWriter.class);
    private static final int BATCH_SIZE = 100;

    private final IngestQueue ingestQueue;
    private final TelemetryRepository repository;

    public DatabaseWriter(IngestQueue ingestQueue, TelemetryRepository repository) {
        this.ingestQueue = ingestQueue;
        this.repository = repository;
    }

    /**
     * Runs every 200ms on Spring's scheduled thread pool.
     * drainTo() is a single lock acquisition that atomically transfers up to
     * BATCH_SIZE elements — far cheaper than 100 individual poll() calls.
     */
    @Scheduled(fixedDelay = 200)
    public void flush() {
        List<EnrichedTelemetry> batch = new ArrayList<>(BATCH_SIZE);
        int drained = ingestQueue.drainTo(batch, BATCH_SIZE);

        if (drained == 0) return;

        List<TelemetryEntity> entities = batch.stream()
            .map(e -> new TelemetryEntity(
                e.raw().nodeId(),
                e.raw().sequenceNumber(),
                e.raw().temperature(),
                e.raw().humidity(),
                e.windowAvgTemperature(),
                e.raw().receivedAt()
            ))
            .toList();

        try {
            repository.saveAll(entities);
            log.debug("Batch-inserted {} records.", drained);
        } catch (Exception ex) {
            // In production: route to a dead-letter store (Kafka/file) instead of losing data.
            log.error("Batch insert failed for {} records. Data lost.", drained, ex);
        }
    }

    /**
     * Called by Spring before the application context closes (SIGTERM / graceful shutdown).
     * Flushes everything remaining in the queue so no in-flight messages are lost.
     */
    @PreDestroy
    public void flushOnShutdown() {
        log.info("Shutdown hook triggered — draining remaining queue (size={}).", ingestQueue.size());
        List<EnrichedTelemetry> remaining = new ArrayList<>();
        ingestQueue.rawQueue().drainTo(remaining); // drain ALL with no cap

        if (remaining.isEmpty()) return;

        List<TelemetryEntity> entities = remaining.stream()
            .map(e -> new TelemetryEntity(
                e.raw().nodeId(),
                e.raw().sequenceNumber(),
                e.raw().temperature(),
                e.raw().humidity(),
                e.windowAvgTemperature(),
                e.raw().receivedAt()
            ))
            .toList();

        try {
            repository.saveAll(entities);
            log.info("Shutdown flush: persisted {} records.", entities.size());
        } catch (Exception ex) {
            log.error("Shutdown flush FAILED. {} records lost.", entities.size(), ex);
        }
    }
}