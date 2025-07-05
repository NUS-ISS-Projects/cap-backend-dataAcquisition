package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.DetonationRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DetonationRepository extends JpaRepository<DetonationRecord, Long> {
    List<DetonationRecord> findByTimestampBetween(Long startTime, Long endTime);
}