package com.cap.dataAcquisition.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class ElectromagneticEmissionsPduRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int emittingSite;
    private int emittingApplication;
    private int emittingEntity;

    @Column(columnDefinition = "BIGINT")
    private long timestamp;
}