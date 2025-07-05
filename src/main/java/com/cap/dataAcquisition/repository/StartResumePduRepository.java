package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.StartResumePduRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface StartResumePduRepository extends JpaRepository<StartResumePduRecord, Long> {
    List<StartResumePduRecord> findByTimestampBetween(Long startTime, Long endTime);
}