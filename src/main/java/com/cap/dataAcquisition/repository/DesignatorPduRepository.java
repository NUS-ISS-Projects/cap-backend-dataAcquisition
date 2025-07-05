package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.DesignatorPduRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface DesignatorPduRepository extends JpaRepository<DesignatorPduRecord, Long> {
    List<DesignatorPduRecord> findByTimestampBetween(Long startTime, Long endTime);
}