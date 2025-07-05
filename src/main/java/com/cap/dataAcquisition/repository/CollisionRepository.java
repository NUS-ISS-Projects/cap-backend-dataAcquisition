package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.CollisionRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CollisionRepository extends JpaRepository<CollisionRecord, Long> {
    List<CollisionRecord> findByTimestampBetween(Long startTime, Long endTime);
}