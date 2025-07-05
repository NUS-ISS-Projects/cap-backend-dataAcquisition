package com.cap.dataAcquisition.repository;

import com.cap.dataAcquisition.model.ElectromagneticEmissionsPduRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ElectromagneticEmissionsPduRepository extends JpaRepository<ElectromagneticEmissionsPduRecord, Long> {
    List<ElectromagneticEmissionsPduRecord> findByTimestampBetween(Long startTime, Long endTime);
}