package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.EntityStateRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EntityStateRepository extends JpaRepository<EntityStateRecord, Long> {
    List<EntityStateRecord> findByTimestampBetween(Long startTime, Long endTime);
}
