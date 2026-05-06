// TelemetryRepository.java
package com.iot.pipeline.repository;

import com.iot.telemetry.model.TelemetryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TelemetryRepository extends JpaRepository<TelemetryEntity, Long> {}