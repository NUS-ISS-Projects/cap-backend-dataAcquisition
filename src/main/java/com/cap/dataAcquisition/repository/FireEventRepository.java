package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.FireEventRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FireEventRepository extends JpaRepository<FireEventRecord, Long> {
    List<FireEventRecord> findByTimestampBetween(Long startTime, Long endTime);
}
