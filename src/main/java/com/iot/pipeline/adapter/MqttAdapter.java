// MqttAdapter.java
package com.iot.pipeline.adapter;

import com.iot.pipeline.model.TelemetryDTO;
import com.iot.pipeline.model.EnrichedTelemetry;
import com.iot.pipeline.pipeline.BinaryDecoder;
import com.iot.pipeline.pipeline.IngestQueue;
import com.iot.pipeline.pipeline.MultiNodeWindowProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
/**
 * Thin pipeline service:
 *   MQTT message → BinaryDecoder → TelemetryEntity → JPA save
 *
 * Responsibilities deliberately kept to three:
 *   1. Manage the Paho MQTT lifecycle (connect / subscribe / disconnect).
 *   2. Delegate raw bytes to BinaryDecoder.
 *   3. Persist the decoded entity via TelemetryRepository.
 *
 * No filtering, no windowing, no aggregation — edge device handles all of that.
 */
@Service
public class MqttMqttAdapter implements MqttCallback {

    private static final Logger log = LoggerFactory.getLogger(MqttPipelineService.class);

    @Value("${mqtt.broker.url}")
    private String brokerUrl;

    @Value("${mqtt.topic}")
    private String topic;

    @Value("${mqtt.client-id:ocs-backend-01}")
    private String clientId;

    private final BinaryDecoder       decoder;
    private final TelemetryRepository repository;

    private MqttClient mqttClient;

    public MqttPipelineService(BinaryDecoder decoder, TelemetryRepository repository) {
        this.decoder    = decoder;
        this.repository = repository;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @PostConstruct
    public void connect() throws MqttException {
        mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        mqttClient.setCallback(this);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);

        mqttClient.connect(options);
        mqttClient.subscribe(topic, 0); // QoS 0 — fire-and-forget sensors
        log.info("MQTT connected: broker={} topic={}", brokerUrl, topic);
    }

    @PreDestroy
    public void disconnect() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect(2_000);
                log.info("MQTT disconnected cleanly.");
            }
        } catch (MqttException ex) {
            log.warn("Error during MQTT disconnect.", ex);
        }
    }

    // -------------------------------------------------------------------------
    // MqttCallback
    // -------------------------------------------------------------------------

    /**
     * Hot path — called on Paho's internal I/O thread.
     *
     * Exceptions are caught and logged; they must never propagate back to Paho,
     * which would log a warning and risk swallowing subsequent messages.
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) {
        try {
            decoder.decode(message.getPayload())          // Optional<TelemetryDTO>
                   .map(this::toEntity)                   // Optional<TelemetryEntity>
                   .ifPresent(repository::save);          // persist if present
        } catch (Exception ex) {
            log.error("Failed to process message on topic [{}]", topic, ex);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("MQTT connection lost — automatic reconnect active.", cause);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Subscriber-only — intentionally empty.
    }

    // -------------------------------------------------------------------------
    // Mapping
    // -------------------------------------------------------------------------

    private TelemetryEntity toEntity(TelemetryDTO dto) {
        return new TelemetryEntity(
            dto.nodeId(),
            dto.temperature(),
            dto.humidity(),
            dto.flags(),
            dto.sequence(),
            dto.deviceTimestamp(),
            dto.receivedAt()
        );
    }
}