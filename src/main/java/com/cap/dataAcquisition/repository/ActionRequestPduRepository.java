package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.ActionRequestPduRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ActionRequestPduRepository extends JpaRepository<ActionRequestPduRecord, Long> {
    List<ActionRequestPduRecord> findByTimestampBetween(Long startTime, Long endTime);
}