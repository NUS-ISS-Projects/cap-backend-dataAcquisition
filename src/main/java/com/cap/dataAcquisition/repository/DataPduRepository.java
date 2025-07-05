package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.DataPduRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DataPduRepository extends JpaRepository<DataPduRecord, Long> {
    List<DataPduRecord> findByTimestampBetween(Long startTime, Long endTime);
}